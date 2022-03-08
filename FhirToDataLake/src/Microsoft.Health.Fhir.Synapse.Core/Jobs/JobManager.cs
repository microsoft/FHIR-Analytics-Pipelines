// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobManager : IDisposable
    {
        private readonly IJobStore _jobStore;
        private readonly JobExecutor _jobExecutor;
        private readonly IFhirSpecificationProvider _fhirSpecificationProvider;
        private readonly JobConfiguration _jobConfiguration;
        private readonly ILogger<JobManager> _logger;

        public JobManager(
            IJobStore jobStore,
            JobExecutor jobExecutor,
            IFhirSpecificationProvider fhirSpecificationProvider,
            IOptions<JobConfiguration> jobConfiguration,
            ILogger<JobManager> logger)
        {
            EnsureArg.IsNotNull(jobStore, nameof(jobStore));
            EnsureArg.IsNotNull(jobExecutor, nameof(jobExecutor));
            EnsureArg.IsNotNull(fhirSpecificationProvider, nameof(fhirSpecificationProvider));
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _jobStore = jobStore;
            _jobExecutor = jobExecutor;
            _fhirSpecificationProvider = fhirSpecificationProvider;
            _jobConfiguration = jobConfiguration.Value;

            _logger = logger;
        }

        /// <summary>
        /// Resume an active job or trigger new job from job store.
        /// and execute the job.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Completed task.</returns>
        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            // Acquire an active job from the job store.
            var job = await _jobStore.AcquireActiveJobAsync(cancellationToken);
            if (job == null)
            {
                job = await CreateNewJobAsync(cancellationToken);
            }

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

        private async Task<Job> CreateNewJobAsync(CancellationToken cancellationToken = default)
        {
            var schedulerSetting = await _jobStore.GetSchedulerMetadataAsync(cancellationToken);

            // If there are unfinishedJobs, continue the progress from a new job.
            if (schedulerSetting?.UnfinishedJobs?.Any() == true)
            {
                var resumedJob = schedulerSetting.UnfinishedJobs.First();
                return new Job(
                    resumedJob.ContainerName,
                    JobStatus.New,
                    resumedJob.ResourceTypes,
                    resumedJob.DataPeriod,
                    DateTimeOffset.UtcNow,
                    resumedJob.ResourceProgresses,
                    null,
                    null,
                    null,
                    null,
                    resumedJob.CompletedResources,
                    resumedJob.Id);
            }

            DateTimeOffset triggerStart = GetTriggerStartTime(schedulerSetting);
            if (triggerStart >= _jobConfiguration.EndTime)
            {
                _logger.LogError("Job has been scheduled to end.");
                throw new StartJobFailedException("Job has been scheduled to end.");
            }

            // End data period for this trigger
            DateTimeOffset triggerEnd = GetTriggerEndTime();

            if (triggerStart >= triggerEnd)
            {
                _logger.LogError("The start time '{triggerStart}' to trigger is in the future.", triggerStart);
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
            await _jobStore.UpdateJobAsync(newJob, cancellationToken);
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
            _jobStore.Dispose();
        }
    }
}
