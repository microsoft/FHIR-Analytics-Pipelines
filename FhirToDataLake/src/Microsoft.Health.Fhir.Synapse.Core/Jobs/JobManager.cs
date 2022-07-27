﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobManager : IDisposable
    {
        private readonly IJobStore _jobStore;
        private readonly IJobExecutor _jobExecutor;
        private readonly ITypeFilterParser _typeFilterParser;
        private readonly JobConfiguration _jobConfiguration;
        private readonly FilterConfiguration _filterConfiguration;
        private readonly ILogger<JobManager> _logger;

        public JobManager(
            IJobStore jobStore,
            IJobExecutor jobExecutor,
            ITypeFilterParser typeFilterParser,
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<FilterConfiguration> filterConfiguration,
            ILogger<JobManager> logger)
        {
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(filterConfiguration, nameof(filterConfiguration));

            _jobConfiguration = jobConfiguration.Value;
            _filterConfiguration = filterConfiguration.Value;

            _jobStore = EnsureArg.IsNotNull(jobStore, nameof(jobStore));
            _jobExecutor = EnsureArg.IsNotNull(jobExecutor, nameof(jobExecutor));
            _typeFilterParser = EnsureArg.IsNotNull(typeFilterParser, nameof(typeFilterParser));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        /// <summary>
        /// Resume an active job or trigger new job from job store.
        /// and execute the job.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Completed task.</returns>
        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Job starts running.");

            // Acquire an active job from the job store.
            var job = await _jobStore.AcquireActiveJobAsync(cancellationToken) ?? await CreateNewJobAsync(cancellationToken);

            if (job == null)
            {
                _logger.LogWarning("Job has been scheduled to end.");

                // release job lock
                Dispose();
            }
            else
            {
                _logger.LogInformation($"The running job id is {job.Id}");

                // Update the running job to job store.
                // For new/resume job, add the created job to job store; For active job, update the last heart beat.
                await _jobStore.UpdateJobAsync(job, cancellationToken);

                try
                {
                    job.Status = JobStatus.Running;
                    await _jobExecutor.ExecuteAsync(job, cancellationToken);
                    job.Status = JobStatus.Succeeded;
                    await _jobStore.CompleteJobAsync(job, cancellationToken);
                }
                catch (Exception exception)
                {
                    job.Status = JobStatus.Failed;
                    job.FailedReason = exception.ToString();
                    await _jobStore.CompleteJobAsync(job, cancellationToken);

                    _logger.LogError(exception, "Process job '{jobId}' failed.", job.Id);
                    throw;
                }
            }
        }

        private async Task<Job> CreateNewJobAsync(CancellationToken cancellationToken = default)
        {
            var schedulerSetting = await _jobStore.GetSchedulerMetadataAsync(cancellationToken);

            // If there are failedJobs, continue the progress from a new job.
            if (schedulerSetting?.FailedJobs?.Any() == true)
            {
                var failedJob = schedulerSetting.FailedJobs.First();
                var resumeJob = Job.Create(
                    failedJob.ContainerName,
                    JobStatus.New,
                    failedJob.DataPeriod,
                    failedJob.FilterInfo,
                    failedJob.Patients,
                    failedJob.NextTaskIndex,
                    failedJob.RunningTasks,
                    failedJob.TotalResourceCounts,
                    failedJob.ProcessedResourceCounts,
                    failedJob.SkippedResourceCounts,
                    failedJob.PatientVersionId,
                    failedJob.Id);

                // update the job id of running task, which is used to generate the result blob file name
                foreach (var runningTask in resumeJob.RunningTasks.Values)
                {
                    runningTask.JobId = resumeJob.Id;
                }

                return resumeJob;
            }

            DateTimeOffset triggerStart = GetTriggerStartTime(schedulerSetting);

            if (triggerStart >= _jobConfiguration.EndTime)
            {
                _logger.LogInformation($"The job trigger start time {triggerStart} is greater than the end time {_jobConfiguration.EndTime} in configuration, no need to start a new job.");
                return null;
            }

            // End data period for this trigger
            DateTimeOffset triggerEnd = GetTriggerEndTime();

            if (triggerStart >= triggerEnd)
            {
                _logger.LogError("The start time '{triggerStart}' to trigger is in the future.", triggerStart);
                throw new StartJobFailedException($"The start time '{triggerStart}' to trigger is in the future.");
            }

            var typeFilters = _typeFilterParser.CreateTypeFilters(
                _filterConfiguration.FilterScope,
                _filterConfiguration.RequiredTypes,
                _filterConfiguration.TypeFilters);

            var processedPatients = schedulerSetting?.ProcessedPatients;

            var filterInfo =
                new FilterInfo(_filterConfiguration.FilterScope, _filterConfiguration.GroupId, _jobConfiguration.StartTime, typeFilters, processedPatients);
            var newJob = Job.Create(
                _jobConfiguration.ContainerName,
                JobStatus.New,
                new DataPeriod(triggerStart, triggerEnd),
                filterInfo);
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
            var nowEnd = DateTimeOffset.UtcNow.AddMinutes(-1 * AzureBlobJobConstants.JobQueryLatencyInMinutes);
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
            _jobStore.Dispose();
        }
    }
}
