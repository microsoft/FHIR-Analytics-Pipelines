// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Azure.Blob;
using Microsoft.Health.Fhir.Synapse.Azure.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Models.Jobs;
using Newtonsoft.Json;
using Timer = System.Timers.Timer;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class AzureBlobJobStore : IJobStore
    {
        private readonly IAzureBlobContainerClient _blobContainerClient;
        private readonly ILogger<AzureBlobJobStore> _logger;

        // When job starts, the process will acquire the lease for expiration
        // of 30 seconds and continues to refresh the lease in 20 seconds interval.
        // When job ends or applicate exists gracefully, the lease will be released.
        private string? _jobLockLease;

        // Timer to renew job lock in a fixed interval
        private readonly Timer _renewLockTimer;

        public AzureBlobJobStore(
            IAzureBlobContainerClientFactory blobContainerFactory,
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<DataLakeStoreConfiguration> storeConfiguration,
            ILogger<AzureBlobJobStore> logger)
        {
            EnsureArg.IsNotNull(blobContainerFactory, nameof(blobContainerFactory));
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(storeConfiguration, nameof(storeConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _blobContainerClient = blobContainerFactory.Create(storeConfiguration.Value.StorageUrl, jobConfiguration.Value.ContainerName);
            _logger = logger;

            _renewLockTimer = new Timer(TimeSpan.FromSeconds(AzureBlobJobConstants.JobLeaseRefreshIntervalInSeconds).TotalMilliseconds);
            _renewLockTimer.Elapsed += async (sender, e) => await RenewJobLockLeaseAsync();
        }

        public async Task<bool> AcquireJobLock(CancellationToken cancellationToken = default)
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

        public async Task<bool> ReleaseJobLock(CancellationToken cancellationToken = default)
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

        public async Task<IEnumerable<Job>> GetActiveJobsAsync(CancellationToken cancellationToken = default)
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

        public async Task<bool> UpdateJobAsync(Job job, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(job, nameof(job));

            job.LastHeartBeat = DateTimeOffset.UtcNow;

            try
            {
                await _blobContainerClient.UpdateBlobAsync(
                    $"{AzureBlobJobConstants.ActiveJobFolder}/{job.Id}.json",
                    job.ToStream(),
                    cancellationToken: cancellationToken);

                return true;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to update job '{job.Id}'.", job.Id);
                return false;
            }
        }

        public async Task<bool> CompleteJobAsync(Job job, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(job, nameof(job));

            if (job.Status != JobStatus.Completed)
            {
                // Should not happen.
                _logger.LogWarning("Job has not been completed yet.");
                throw new ArgumentException("Input job to complete is not in completed state.");
            }

            var completedBlobName = job.Status == JobStatus.Completed ?
                GetJobBlobName(job, AzureBlobJobConstants.CompletedJobFolder) : GetJobBlobName(job, AzureBlobJobConstants.FailedJobFolder);

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

                // Add job to completed job list.
                await _blobContainerClient.UpdateBlobAsync(
                    completedBlobName,
                    job.ToStream(),
                    cancellationToken);

                // Remove job from active job list.
                await _blobContainerClient.DeleteBlobAsync(GetJobBlobName(job, AzureBlobJobConstants.ActiveJobFolder), cancellationToken);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to complete job '{job.Id}'", job.Id);
                throw new CompleteJobFailedException($"Complete job '{job.Id}' failed.", ex);
            }
        }

        public async Task<SchedulerMetadata> GetSchedulerMetadata(CancellationToken cancellationToken = default)
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

        private string GetJobBlobName(Job job, string prefix)
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

    }
}
