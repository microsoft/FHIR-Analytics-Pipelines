// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Azure;
using Microsoft.Health.Fhir.Synapse.Azure.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Fhir;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Scheduler.Exceptions;
using Microsoft.Health.Fhir.Synapse.Scheduler.Extensions;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Jobs
{
    public class JobStore : IJobStore
    {
        private readonly IAzureBlobContainerClient _blobContainerClient;
        private readonly IFhirSpecificationProvider _fhirSpecificationProvider;
        private readonly JobConfiguration _jobConfiguration;
        private readonly ILogger<JobStore> _logger;

        // Lease id for running job blob.
        // When executing, the process will hold the lease for infinite expiration.
        // When application exits, the lease will be released when JobManager dispose.
        // A job file with active lease and heartbeat in last 10 minutes will be considered active.
        private string? _runningJobLeaseId;

        public JobStore(
            IAzureBlobContainerClientFactory blobContainerFactory,
            IFhirSpecificationProvider fhirSpecificationProvider,
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<DataLakeStoreConfiguration> storeConfiguration,
            ILogger<JobStore> logger)
        {
            EnsureArg.IsNotNull(blobContainerFactory, nameof(blobContainerFactory));
            EnsureArg.IsNotNull(fhirSpecificationProvider, nameof(fhirSpecificationProvider));
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(storeConfiguration, nameof(storeConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _fhirSpecificationProvider = fhirSpecificationProvider;
            _jobConfiguration = jobConfiguration.Value;
            _blobContainerClient = blobContainerFactory.Create(storeConfiguration.Value.StorageUrl, _jobConfiguration.ContainerName);
            _logger = logger;
        }

        public async Task<Job> StartJobAsync(CancellationToken cancellationToken = default)
        {
            // Fetch running job from blob
            var job = await FromBlob<Job>(JobConstants.RunningJobName, cancellationToken);

            // No job is currently running, create new.
            if (job == null)
            {
                return await CreateNewJob(cancellationToken);
            }

            // job has finished, complete original job and create new job.
            if (job.Status == JobStatus.Completed
                || job.Status == JobStatus.Failed)
            {
                try
                {
                    await CompleteJobAsync(job, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to complete already finished job '{job.Id}'", job.Id);
                    throw new StartJobFailedException($"Failed to complete job '{job.Id}'. Reason: '{ex}'.", ex, JobErrorCode.SaveJobFailed);
                }

                return await CreateNewJob(cancellationToken);
            }

            // Resume the running job.
            _runningJobLeaseId = await _blobContainerClient.AcquireLeaseAsync(JobConstants.RunningJobName, _runningJobLeaseId, TimeSpan.FromSeconds(-1), false, cancellationToken);
            if (string.IsNullOrEmpty(_runningJobLeaseId) && job.LastHeartBeat.AddMinutes(JobConstants.JobActiveThresholdInMinutes) > DateTimeOffset.UtcNow)
            {
                // Job running, throws.
                _logger.LogWarning("Resume job conflicts. An active job is running.");
                throw new StartJobFailedException("Resume job conflicts. An active job is running.", JobErrorCode.ResumeJobConflict);
            }

            if (string.IsNullOrEmpty(_runningJobLeaseId))
            {
                // Job is inactive, break lease and acquire new lease
                _runningJobLeaseId = await _blobContainerClient.AcquireLeaseAsync(JobConstants.RunningJobName, _runningJobLeaseId, TimeSpan.FromSeconds(-1), true, cancellationToken);
            }

            // Return resumed job.
            return job;
        }

        public async Task<Job> CompleteJobAsync(Job job, CancellationToken cancellationToken = default)
        {
            if (job.Status != JobStatus.Completed
                && job.Status != JobStatus.Failed)
            {
                // Should not happen.
                _logger.LogWarning("Job has not been completed yet.");
                throw new ArgumentException("Input job to complete is not in completed state.");
            }

            var completedFolderName = job.Status == JobStatus.Completed ?
                JobConstants.CompletedJobFolder : JobConstants.FailedJobFolder;
            var completedBlobName = $"{completedFolderName}/{job.Id}";

            job.LastHeartBeat = DateTimeOffset.UtcNow;
            job.CompletedTime = job.LastHeartBeat;

            try
            {
                await _blobContainerClient.UploadBlobAsync(
                    completedBlobName,
                    job.ToStream(),
                    true,
                    cancellationToken: cancellationToken);
                await _blobContainerClient.DeleteBlobAsync(JobConstants.RunningJobName, _runningJobLeaseId, cancellationToken);
                _runningJobLeaseId = null;

                return job;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to complete job '{job.Id}'", job.Id);
                throw new CompleteJobFailedException($"Complete job '{job.Id}' failed.", ex);
            }
        }

        public async Task<Job?> UpdateJobAsync(Job job, CancellationToken cancellationToken = default)
        {
            job.LastHeartBeat = DateTimeOffset.Now;
            try
            {
                await _blobContainerClient.UploadBlobAsync(
                    JobConstants.RunningJobName,
                    job.ToStream(),
                    true,
                    _runningJobLeaseId,
                    cancellationToken);

                return job;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to update job '{job.Id}'", job.Id);
                return null;
            }
        }

        public async Task<bool> ReleaseJobAsync(CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(_runningJobLeaseId))
            {
                return true;
            }

            bool result = await _blobContainerClient.ReleaseLeaseAsync(JobConstants.RunningJobName, _runningJobLeaseId, cancellationToken);
            if (result)
            {
                _runningJobLeaseId = null;
            }

            return result;
        }

        /// <summary>
        /// Get running job from cloud.
        /// Returns null if no job is running.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>running job.</returns>
        private async Task<T?> FromBlob<T>(string blobName, CancellationToken cancellationToken)
        {
            try
            {
                var blobStream = await _blobContainerClient.GetBlobAsync(blobName, cancellationToken);
                if (blobStream == null)
                {
                    return default;
                }

                using var streamReader = new StreamReader(blobStream);
                var content = streamReader.ReadToEnd();
                return JsonConvert.DeserializeObject<T>(content);
            }
            catch (AzureBlobOperationFailedException blobEx)
            {
                _logger.LogWarning(blobEx, "Get blob file '{blob}' failed.", blobName);
                throw new StartJobFailedException($"Get blob file {blobName} failed.", blobEx, JobErrorCode.JobFileReadError);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Parse blob file '{blob}' failed.", blobName);
                throw new StartJobFailedException($"Parse blob file '{blobName}' failed.", ex, JobErrorCode.JobFileReadError);
            }
        }

        /// <summary>
        /// Create new Job.
        /// If we have scheduled to data end, return null.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>new job created.</returns>
        private async Task<Job> CreateNewJob(CancellationToken cancellationToken)
        {
            var jobConfig = await FromBlob<JobConfiguration>(JobConstants.JobConfigName, cancellationToken);
            DateTimeOffset jobStart = jobConfig?.LastScheduledTimestamp ?? _jobConfiguration.StartTime;

            // End data period for this trigger
            DateTimeOffset triggerEnd = GetTiggerEndTime();

            if (jobStart >= triggerEnd)
            {
                _logger.LogInformation("Job has been scheduled to end.");
                throw new StartJobFailedException("Job has been scheduled to end.", JobErrorCode.NoJobToSchedule);
            }

            IEnumerable<string> resourceTypes = _jobConfiguration.ResourceTypeFilters;
            if (resourceTypes == null || !resourceTypes.Any())
            {
                resourceTypes = _fhirSpecificationProvider.GetAllResourceTypes();
            }

            var newJob = new Job(
                _jobConfiguration.ContainerName,
                JobStatus.New,
                resourceTypes,
                new DataPeriod(jobStart, triggerEnd),
                DateTimeOffset.Now);
            await SaveBlob(newJob, JobConstants.RunningJobName, false, cancellationToken);

            if (jobConfig == null)
            {
                jobConfig = _jobConfiguration;
            }

            jobConfig.LastScheduledTimestamp = newJob.DataPeriod.End;
            await SaveBlob(jobConfig, JobConstants.JobConfigName, true, cancellationToken);

            // Acquire lease for new job.
            _runningJobLeaseId = await _blobContainerClient.AcquireLeaseAsync(JobConstants.RunningJobName, _runningJobLeaseId, TimeSpan.FromSeconds(-1), false, cancellationToken);

            return newJob;
        }

        // Job end time could be null (which means runs forever) or a timestamp in the future like 2120/01/01.
        // In this case, we will create a job to run with end time earlier that current timestamp.
        // Also, FHIR data use processing time as lastUpdated timestamp, there might be some latency when saving to data store.
        // Here we add a JobEndTimeLatencyInMinutes latency to avoid data missing due to latency in creation.
        private DateTimeOffset GetTiggerEndTime()
        {
            // Add two minutes latency here to allow latency in saving resources to database.
            var nowTime = DateTimeOffset.Now.AddMinutes(-1 * JobConstants.JobEndTimeLatencyInMinutes);
            if (_jobConfiguration.EndTime != null
                && nowTime > _jobConfiguration.EndTime)
            {
                return _jobConfiguration.EndTime.Value;
            }
            else
            {
                return nowTime;
            }
        }

        private async Task SaveBlob<T>(T data, string blobName, bool update = false, CancellationToken cancellationToken = default)
        {
            var content = JsonConvert.SerializeObject(data);
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));

            try
            {
                await _blobContainerClient.UploadBlobAsync(
                    blobName,
                    stream,
                    update,
                    cancellationToken: cancellationToken);
            }
            catch (AzureBlobOperationFailedException blobEx)
            {
                throw new StartJobFailedException($"Failed to save blob '{blobName}', reason '{blobEx}'", blobEx, JobErrorCode.SaveJobFailed);
            }
        }
    }
}
