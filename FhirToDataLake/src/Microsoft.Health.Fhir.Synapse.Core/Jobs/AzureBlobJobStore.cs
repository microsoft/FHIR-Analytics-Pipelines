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
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions;
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
        private string _jobLockLease;

        // Timer to renew job lock in a fixed interval
        private readonly Timer _renewLockTimer;

        // Concurrency to control how many folders are processed concurrently.
        private const int StageFolderConcurrency = 20;

        // Staged data folder path: "staging/{jobid}/{resourceType}/{year}/{month}/{day}"
        // Committed data file path: "result/{resourceType}/{year}/{month}/{day}/{jobid}"
        private readonly Regex _stagingDataFolderRegex = new Regex(AzureStorageConstants.StagingFolderName + @"/[a-z0-9]{32}/(?<partition>[A-Za-z]+/\d{4}/\d{2}/\d{2})$");

        // Staged data file path: "staging/{jobid}/{resourceType}/{year}/{month}/{day}/{resourceType}_{jobId}_{partId}.parquet"
        // Committed data file path: "result/{resourceType}/{year}/{month}/{day}/{jobid}/{resourceType}_{jobId}_{partId}.parquet"
        private readonly Regex _stagingDataBlobRegex = new Regex(AzureStorageConstants.StagingFolderName + @"/[a-z0-9]{32}/[A-Za-z]+/\d{4}/\d{2}/\d{2}/(?<resource>[A-Za-z]+)_[a-z0-9]{32}_(?<partId>\d+).parquet$");

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

        public async Task<Job> AcquireActiveJobAsync(CancellationToken cancellationToken = default)
        {
            var lockAcquired = await TryAcquireJobLockAsync(cancellationToken);
            if (!lockAcquired)
            {
                _logger.LogError("Another job is already started. Please try later.");
                throw new StartJobFailedException("Another job is already started. Please try later.");
            }

            var activeJobs = await GetActiveJobsAsync(cancellationToken);
            Job job = null;

            foreach (var activeJob in activeJobs)
            {
                // Complete job if it's already succeeded or failed.
                if (activeJob.Status == JobStatus.Succeeded
                    || activeJob.Status == JobStatus.Failed)
                {
                    _logger.LogWarning("Job '{id}' has already finished.", activeJob.Id);
                    await CompleteJobAsyncInternal(activeJob, false, cancellationToken);
                }
                else
                {
                    job = activeJob;
                    break;
                }
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

            await CompleteJobAsyncInternal(job, true, cancellationToken);
        }

        public async Task<SchedulerMetadata> GetSchedulerMetadataAsync(CancellationToken cancellationToken = default)
        {
            return await FromBlobAsync<SchedulerMetadata>(AzureBlobJobConstants.SchedulerMetadataFileName, cancellationToken);
        }

        private async Task<bool> TryAcquireJobLockAsync(CancellationToken cancellationToken = default)
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
                null,
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

        private async Task<bool> TryReleaseJobLockAsync(CancellationToken cancellationToken = default)
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
                var job = await FromBlobAsync<Job>(blob, cancellationToken);
                if (job != null)
                {
                    jobs.Add(job);
                }
            }

            return jobs;
        }

        private async Task CompleteJobAsyncInternal(Job job, bool releaseLock, CancellationToken cancellationToken)
        {
            if (job.Status != JobStatus.Succeeded &&
                job.Status != JobStatus.Failed)
            {
                // Should not happen.
                _logger.LogError("Input job to complete is not in succeeded or failed state.");
                throw new ArgumentException("Input job to complete is not in succeeded or failed state.");
            }

            await CommitJobDataAsync(job, cancellationToken);
            job.LastHeartBeat = DateTimeOffset.UtcNow;
            job.CompletedTime = job.LastHeartBeat;

            try
            {
                // Update scheduler setting
                var schedulerSetting = await GetSchedulerMetadataAsync(cancellationToken);
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

                // Remove the job information that have been resumed and executed.
                // If the current job fails, save it to unfinishedJob list.
                var unfinishedJobs = schedulerSetting.UnfinishedJobs.ToList();
                unfinishedJobs.RemoveAll(unfinished => unfinished.Id == job.ResumedJobId);

                if (job.Status == JobStatus.Failed)
                {
                    unfinishedJobs.Add(job);
                }

                schedulerSetting.UnfinishedJobs = unfinishedJobs;
                await SaveSchedulerMetadataAsync(schedulerSetting, cancellationToken);

                // Add job to completed job list.
                var completedBlobName = GetJobBlobName(job, AzureBlobJobConstants.CompletedJobFolder);
                await _blobContainerClient.UpdateBlobAsync(
                    completedBlobName,
                    job.ToStream(),
                    cancellationToken);

                // Remove job from active job list.
                await _blobContainerClient.DeleteBlobAsync(GetJobBlobName(job, AzureBlobJobConstants.ActiveJobFolder), cancellationToken);

                if (releaseLock)
                {
                    await TryReleaseJobLockAsync(cancellationToken);
                }

                _logger.LogInformation("Complete job '{job.Id}' successfully.", job.Id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to complete job '{job.Id}'", job.Id);
                throw new CompleteJobFailedException($"Complete job '{job.Id}' failed.", ex);
            }
        }

        /// <summary>
        /// We first get all pathItems from staging folder.
        /// The result contains all subfolders and blobs like
        ///   "staging/jobid" directory
        ///   "staging/jobid/resourceType" directory
        ///   "staging/jobid/resourceType/year" directory
        ///   "staging/jobid/resourceType/year/month" directory
        ///   "staging/jobid/resourceType/year/month/day1" directory
        ///   "staging/jobid/resourceType/year/month/day1/resourceType_jobid_00001.parquet" blob
        ///   "staging/jobid/resourceType/year/month/day1/resourceType_jobid_00002.parquet" blob
        ///   "staging/jobid/resourceType/year/month/day2" directory
        ///   "staging/jobid/resourceType/year/month/day2/resourceType_jobid_00001.parquet" blob
        ///   "staging/jobid/resourceType/year/month/day2/resourceType_jobid_00002.parquet" blob
        /// For blobs, we need to check the partId to ensure only partIds recorded in the job status are committed in case there are some dangling files.
        /// For directories, we need to find all leaf directory and map to the target directory in result folder.
        /// Then rename the source directory to target directory.
        /// </summary>
        /// <param name="job">input job.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>completed task.</returns>
        private async Task CommitJobDataAsync(Job job, CancellationToken cancellationToken = default)
        {
            var stagingFolder = $"{AzureStorageConstants.StagingFolderName}/{job.Id}";
            var directoryPairs = new List<Tuple<string, string>>();

            await foreach (var path in _blobContainerClient.ListPathsAsync(stagingFolder, cancellationToken))
            {
                if (path.IsDirectory == true)
                {
                    // Record all directories that need to commit.
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

        private async Task SaveSchedulerMetadataAsync(SchedulerMetadata setting, CancellationToken cancellationToken = default)
        {
            var content = JsonConvert.SerializeObject(setting);
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
            await _blobContainerClient.UpdateBlobAsync(
                AzureBlobJobConstants.SchedulerMetadataFileName,
                stream,
                cancellationToken);
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
        private async Task<T> FromBlobAsync<T>(string blobName, CancellationToken cancellationToken)
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

        private static string GetJobBlobName(Job job, string prefix)
        {
            return $"{prefix}/{job.Id}.json";
        }

        public void Dispose()
        {
            TryReleaseJobLockAsync().GetAwaiter().GetResult();
        }
    }
}
