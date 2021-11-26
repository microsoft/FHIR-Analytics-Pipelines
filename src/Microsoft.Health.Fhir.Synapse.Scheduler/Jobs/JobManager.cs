// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Scheduler.Exceptions;
using Microsoft.Health.Fhir.Synapse.Scheduler.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Jobs
{
    public class JobManager : IDisposable, IAsyncDisposable
    {
        private readonly IJobStore _jobStore;
        private readonly ITaskExecutor _taskExecutor;
        private readonly JobSchedulerConfiguration _schedulerConfig;
        private readonly ILogger<JobManager> _logger;

        public JobManager(
            IJobStore jobStore,
            ITaskExecutor taskExecutor,
            IOptions<JobSchedulerConfiguration> schedulerConfig,
            ILogger<JobManager> logger)
        {
            _jobStore = jobStore;
            _taskExecutor = taskExecutor;
            _schedulerConfig = schedulerConfig.Value;
            _logger = logger;
        }

        /// <summary>
        /// Create a new job or resume a running job and schedule tasks to run the job.
        /// </summary>
        public async Task ExecuteAsync(CancellationToken cancellationToken = default)
        {
            Job job;
            try
            {
                job = await _jobStore.StartJobAsync(cancellationToken);
            }
            catch (StartJobFailedException startException)
            {
                if (startException.Code == JobErrorCode.NoJobToSchedule
                    || startException.Code == JobErrorCode.ResumeJobConflict)
                {
                    // Directly return on schedule failures.
                    _logger.LogInformation("Scheduling job completed.");
                    return;
                }

                // Throw on other error types.
                throw;
            }
            catch (Exception ex)
            {
                // Create job failed.
                _logger.LogError(ex, "Create or resume job failed. Reason: {}", ex);
                throw new StartJobFailedException("Start job failed due to unhandled exception.", ex, JobErrorCode.UnhandledError);
            }

            _logger.LogInformation("Start processing job {jobId}.", job.Id);

            job.Status = JobStatus.Running;
            await _jobStore.UpdateJobAsync(job, cancellationToken);

            try
            {
                await ScheduleTasks(job, cancellationToken);
                job.Status = JobStatus.Completed;
                await _jobStore.CompleteJobAsync(job, cancellationToken);
            }
            catch (Exception exception)
            {
                job.Status = JobStatus.Failed;
                job.FailedReason = exception.ToString();
                _logger.LogError(exception, "Process job {jobId} failed.", job.Id);
                await _jobStore.CompleteJobAsync(job, cancellationToken);
                throw;
            }

            _logger.LogInformation("Finished processing job {jobId}", job.Id);
        }

        private async Task ScheduleTasks(Job job, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start scheduling job {jobId}", job.Id);

            var progress = new Progress<TaskContext>(async context =>
            {
                job.ResourceProgresses[context.ResourceType] = context.ContinuationToken;
                job.TotalResourceCounts[context.ResourceType] = context.SearchCount;
                job.ProcessedResourceCounts[context.ResourceType] = context.ProcessedCount;
                job.SkippedResourceCounts[context.ResourceType] = context.SkippedCount;
                job.PartIds[context.ResourceType] = context.PartId;

                await _jobStore.UpdateJobAsync(job, cancellationToken);
            });

            var tasks = new List<Task>();
            foreach (var resourceType in job.ResourceTypes)
            {
                if (tasks.Count >= _schedulerConfig.MaxConcurrencyCount)
                {
                    var finishedTask = await Task.WhenAny(tasks);
                    if (finishedTask.IsFaulted)
                    {
                        _logger.LogError("Process task failed.");
                        throw new ExecuteTaskFailedException("Task execution failed", finishedTask.Exception);
                    }

                    tasks.Remove(finishedTask);
                }

                var context = TaskContext.Create(resourceType, job);
                tasks.Add(Task.Run(async () => await _taskExecutor.ExecuteAsync(context, progress, cancellationToken)));

                _logger.LogInformation("Start processing resource {resourceType}", resourceType);
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                _logger.LogError("Process task failed.");
                throw new ExecuteTaskFailedException("Task execution failed", ex);
            }

            _logger.LogInformation("Finish scheduling job {jobId}", job.Id);
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            await DisposeAsyncCore();

            Dispose();
            #pragma warning disable CA1816 // Dispose methods should call SuppressFinalize
            GC.SuppressFinalize(this);
            #pragma warning restore CA1816 // Dispose methods should call SuppressFinalize
        }

        protected virtual async ValueTask DisposeAsyncCore()
        {
            await _jobStore.ReleaseJobAsync();
        }
    }
}
