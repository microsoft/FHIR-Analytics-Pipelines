// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.DataClient.Fhir;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions;
using Newtonsoft.Json;
using Timer = System.Timers.Timer;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class AzureBlobJobStore : IJobStore, IDisposable, IAsyncDisposable
    {
        private readonly IAzureBlobContainerClient _blobContainerClient;
        private readonly IFhirSpecificationProvider _fhirSpecificationProvider;
        private readonly JobConfiguration _jobConfiguration;
        private readonly ILogger<AzureBlobJobStore> _logger;

        // When job starts, the process will acquire the lease for expiration
        // of 30 seconds and continues to refresh the lease in 20 seconds interval.
        // When job ends or applicate exists gracefully, the lease will be released.
        private string _jobLockLease;

        // Timer to renew job lock in a fixed interval
        private readonly Timer _renewLockTimer;

        // Concurrency to control how many folders are processed concurrently.
        private const int StageFolderConcurrency = 20;
        private readonly Regex _stagingDataFolderRegex = new Regex(AzureStorageConstants.StagingFolderName + @"/[a-z0-9]{32}/(?<partition>[A-Za-z]+/\d{4}/\d{2}/\d{2})$");
        private readonly Regex _stagingDataBlobRegex = new Regex(AzureStorageConstants.StagingFolderName + @"/[a-z0-9]{32}/[A-Za-z]+/\d{4}/\d{2}/\d{2}/(?<resource>[A-Za-z]+)_[a-z0-9]{32}_(?<partId>\d+).parquet$");

        public AzureBlobJobStore(
            IAzureBlobContainerClientFactory blobContainerFactory,
            IFhirSpecificationProvider fhirSpecificationProvider,
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<DataLakeStoreConfiguration> storeConfiguration,
            ILogger<AzureBlobJobStore> logger)
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

            _renewLockTimer = new Timer(TimeSpan.FromSeconds(AzureBlobJobConstants.JobLeaseRefreshIntervalInSeconds).TotalMilliseconds);
            _renewLockTimer.Elapsed += async (sender, e) => await RenewJobLockLeaseAsync();
        }

        public async Task<Job> AcquireJobAsync(CancellationToken cancellationToken = default)
        {
            var lockAcquired = await TryAcquireJobLock(cancellationToken);
            if (!lockAcquired)
            {
                _logger.LogWarning("Start job conflicted. Failed to acquire job lock.");
                throw new StartJobFailedException("Start job conflicted. Will skip this trigger.");
            }

            Job job = null;
            var activeJobs = await GetActiveJobsAsync(cancellationToken);
            if (activeJobs.Any())
            {
                // Resume an active job.
                job = activeJobs.First();

                if (job.Status == JobStatus.Succeeded)
                {
                    _logger.LogWarning("Job '{id}' has already succeeded.", job.Id);
                    await CompleteJobInternal(job, false, cancellationToken);
                }
                else
                {
                    // Resume an inactive/failed job.
                    job.Status = JobStatus.Running;
                    job.FailedReason = null;
                }
            }

            // No active job available, start new trigger.
            if (job == null)
            {
                job = await CreateNewJob(cancellationToken);
            }

            return job;
        }

        public async Task UpdateJobAsync(Job job, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(job, nameof(job));

            job.LastHeartBeat = DateTimeOffset.UtcNow;

            try
            {
                await _blobContainerClient.UpdateBlobAsync(
                    $"{AzureBlobJobConstants.ActiveJobFolder}/{job.Id}.json",
                    job.ToStream(),
                    cancellationToken: cancellationToken);

                _logger.LogInformation("Update job '{job.Id}' successfully.", job.Id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to update job '{job.Id}'", job.Id);
                throw;
            }
        }

        public async Task CompleteJobAsync(Job job, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(job, nameof(job));

            await CompleteJobInternal(job, true, cancellationToken);
        }

        private async Task CommitJobDataAsync(Job job, CancellationToken cancellationToken = default)
        {
            var stagingFolder = $"{AzureStorageConstants.StagingFolderName}/{job.Id}";
            var directoryPairs = new List<Tuple<string, string>>();

            var pathItems = await _blobContainerClient.ListPathsAsync(stagingFolder, cancellationToken);
            foreach (var path in pathItems)
            {
                if (path.IsDirectory == true)
                {
                    var match = _stagingDataFolderRegex.Match(path.Name);
                    if (match.Success)
                    {
                        var destination = $"{AzureStorageConstants.ResultFolderName}/{match.Groups["partition"].Value}/{job.Id}";
                        directoryPairs.Add(new Tuple<string, string>(path.Name, destination));
                    }
                }
                else
                {
                    // remove parquet files not saved in this job.
                    var match = _stagingDataBlobRegex.Match(path.Name);
                    if (match.Success)
                    {
                        var resource = match.Groups["resource"].Value;
                        var partId = int.Parse(match.Groups["partId"].Value);
                        if (partId >= job.PartIds[resource])
                        {
                            await _blobContainerClient.DeleteBlobAsync(path.Name, cancellationToken);
                        }
                    }
                }
            }

            // move directories from staging to result folder.
            var moveTasks = new List<Task>();
            foreach (var pair in directoryPairs)
            {
                if (moveTasks.Count >= StageFolderConcurrency)
                {
                    var completedTask = await Task.WhenAny(moveTasks);
                    await completedTask;
                    moveTasks.Remove(completedTask);
                }

                moveTasks.Add(_blobContainerClient.MoveDirectoryAsync(pair.Item1, pair.Item2, cancellationToken));
            }

            await Task.WhenAll(moveTasks);

            // delete staging folder when success.
            await _blobContainerClient.DeleteDirectoryIfExistsAsync(stagingFolder, cancellationToken);
        }

        private async Task<bool> TryAcquireJobLock(CancellationToken cancellationToken = default)
        {
            var blobName = AzureBlobJobConstants.JobLockFileName;

            bool lockExists = await _blobContainerClient.BlobExistsAsync(blobName, cancellationToken);
            if (!lockExists)
            {
                var stream = new MemoryStream();
                await _blobContainerClient.CreateBlobAsync(blobName, stream, cancellationToken);
            }

            _jobLockLease = await _blobContainerClient.AcquireLeaseAsync(
                blobName,
                _jobLockLease,
                TimeSpan.FromSeconds(AzureBlobJobConstants.JobLeaseExpirationInSeconds),
                cancellationToken);

            // Acquire lease failed, return false.
            if (string.IsNullOrEmpty(_jobLockLease))
            {
                return false;
            }

            // Start renew timer.
            _renewLockTimer.Start();
            return true;
        }

        private async Task<bool> TryReleaseJobLock(CancellationToken cancellationToken = default)
        {
            _renewLockTimer.Stop();

            var blobName = AzureBlobJobConstants.JobLockFileName;

            bool lockExists = await _blobContainerClient.BlobExistsAsync(blobName, cancellationToken);
            if (!lockExists || string.IsNullOrEmpty(_jobLockLease))
            {
                return true;
            }

            var result = await _blobContainerClient.ReleaseLeaseAsync(blobName, _jobLockLease, cancellationToken);
            if (result)
            {
                _jobLockLease = null;
            }

            return result;
        }

        private async Task<IEnumerable<Job>> GetActiveJobsAsync(CancellationToken cancellationToken = default)
        {
            var jobs = new List<Job>();
            var jobBlobs = await _blobContainerClient.ListBlobsAsync(AzureBlobJobConstants.ActiveJobFolder, cancellationToken);
            foreach (var blob in jobBlobs)
            {
                var job = await FromBlob<Job>(blob, cancellationToken);
                if (job != null)
                {
                    jobs.Add(job);
                }
            }

            return jobs;
        }

        private async Task CompleteJobInternal(Job job, bool releaseLock, CancellationToken cancellationToken)
        {
            if (job.Status != JobStatus.Succeeded)
            {
                // Should not happen.
                _logger.LogWarning("Job has not succeeded yet.");
                throw new ArgumentException("Input job to complete is not in succeeded state.");
            }

            await CommitJobDataAsync(job, cancellationToken);
            job.LastHeartBeat = DateTimeOffset.UtcNow;
            job.CompletedTime = job.LastHeartBeat;

            try
            {
                // Update scheduler setting
                var schedulerSetting = await GetSchedulerMetadata(cancellationToken);
                if (schedulerSetting != null)
                {
                    schedulerSetting.LastScheduledTimestamp = job.DataPeriod.End;
                }
                else
                {
                    schedulerSetting = new SchedulerMetadata
                    {
                        LastScheduledTimestamp = job.DataPeriod.End,
                    };
                }

                await SaveSchedulerMetadata(schedulerSetting, cancellationToken);

                var completedBlobName = GetJobBlobName(job, AzureBlobJobConstants.CompletedJobFolder);
                // Add job to completed job list.
                await _blobContainerClient.UpdateBlobAsync(
                    completedBlobName,
                    job.ToStream(),
                    cancellationToken);

                // Remove job from active job list.
                await _blobContainerClient.DeleteBlobAsync(GetJobBlobName(job, AzureBlobJobConstants.ActiveJobFolder), cancellationToken);

                if (releaseLock)
                {
                    await TryReleaseJobLock(cancellationToken);
                }

                _logger.LogInformation("Complete job '{job.Id}' successfully.", job.Id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to complete job '{job.Id}'", job.Id);
                throw new CompleteJobFailedException($"Complete job '{job.Id}' failed.", ex);
            }
        }

        private async Task<SchedulerMetadata> GetSchedulerMetadata(CancellationToken cancellationToken = default)
        {
            return await FromBlob<SchedulerMetadata>(AzureBlobJobConstants.SchedulerMetadataFileName, cancellationToken);
        }

        private async Task SaveSchedulerMetadata(SchedulerMetadata setting, CancellationToken cancellationToken = default)
        {
            var content = JsonConvert.SerializeObject(setting);
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
            await _blobContainerClient.UpdateBlobAsync(
                AzureBlobJobConstants.SchedulerMetadataFileName,
                stream,
                cancellationToken);
        }

        private static string GetJobBlobName(Job job, string prefix)
        {
            return $"{prefix}/{job.Id}.json";
        }

        private async Task RenewJobLockLeaseAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                _jobLockLease = await _blobContainerClient.RenewLeaseAsync(AzureBlobJobConstants.JobLockFileName, _jobLockLease, cancellationToken);
                _logger.LogInformation("Successfully renewed job lease.");
            }
            catch
            {
                _logger.LogError("Failed to renew job lease.");
                throw;
            }
        }

        /// <summary>
        /// Get blob content and parse blob.
        /// Returns null if blob not exists.
        /// </summary>
        /// <param name="blobName">blob name.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>result object.</returns>
        private async Task<T> FromBlob<T>(string blobName, CancellationToken cancellationToken)
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
                throw new StartJobFailedException($"Get blob file {blobName} failed.", blobEx);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Parse blob file '{blob}' failed.", blobName);
                throw new StartJobFailedException($"Parse blob file '{blobName}' failed.", ex);
            }
        }

        private async Task<Job> CreateNewJob(CancellationToken cancellationToken = default)
        {
            var schedulerSetting = await GetSchedulerMetadata(cancellationToken);
            DateTimeOffset triggerStart = GetTriggerStartTime(schedulerSetting);
            if (triggerStart >= _jobConfiguration.EndTime)
            {
                _logger.LogInformation("Job has been scheduled to end.");
                throw new StartJobFailedException("Job has been scheduled to end.");
            }

            // End data period for this trigger
            DateTimeOffset triggerEnd = GetTriggerEndTime();

            if (triggerStart >= triggerEnd)
            {
                _logger.LogInformation("The start time '{triggerStart}' to trigger is in the future.", triggerStart);
                throw new StartJobFailedException($"The start time '{triggerStart}' to trigger is in the future.");
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
                new DataPeriod(triggerStart, triggerEnd),
                DateTimeOffset.UtcNow);
            await UpdateJobAsync(newJob, cancellationToken);
            return newJob;
        }

        private DateTimeOffset GetTriggerStartTime(SchedulerMetadata schedulerSetting)
        {
            var lastScheduledTo = schedulerSetting?.LastScheduledTimestamp;
            return lastScheduledTo ?? _jobConfiguration.StartTime;
        }

        // Job end time could be null (which means runs forever) or a timestamp in the future like 2120/01/01.
        // In this case, we will create a job to run with end time earlier that current timestamp.
        // Also, FHIR data use processing time as lastUpdated timestamp, there might be some latency when saving to data store.
        // Here we add a JobEndTimeLatencyInMinutes latency to avoid data missing due to latency in creation.
        private DateTimeOffset GetTriggerEndTime()
        {
            // Add two minutes latency here to allow latency in saving resources to database.
            var nowEnd = DateTimeOffset.Now.AddMinutes(-1 * AzureBlobJobConstants.JobQueryLatencyInMinutes);
            if (_jobConfiguration.EndTime != null
                && nowEnd > _jobConfiguration.EndTime)
            {
                return _jobConfiguration.EndTime.Value;
            }
            else
            {
                return nowEnd;
            }
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            await DisposeAsyncCore();

            Dispose();
            GC.SuppressFinalize(this);
        }

        protected virtual async ValueTask DisposeAsyncCore()
        {
            await TryReleaseJobLock();
        }
    }
}
