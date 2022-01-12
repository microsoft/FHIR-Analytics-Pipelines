// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobExecutor
    {
        private readonly IJobStore _jobStore;
        private readonly ITaskExecutor _taskExecutor;
        private readonly JobSchedulerConfiguration _schedulerConfiguration;
        private readonly ILogger<JobExecutor> _logger;

        public JobExecutor(
            IJobStore jobStore,
            ITaskExecutor taskExecutor,
            IOptions<JobSchedulerConfiguration> schedulerConfiguration,
            ILogger<JobExecutor> logger)
        {
            EnsureArg.IsNotNull(jobStore, nameof(jobStore));
            EnsureArg.IsNotNull(taskExecutor, nameof(taskExecutor));
            EnsureArg.IsNotNull(schedulerConfiguration, nameof(schedulerConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _jobStore = jobStore;
            _taskExecutor = taskExecutor;
            _schedulerConfiguration = schedulerConfiguration.Value;
            _logger = logger;
        }

        public async Task ExecuteAsync(Job job, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start executing job '{jobId}'.", job.Id);

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
                if (tasks.Count >= _schedulerConfiguration.MaxConcurrencyCount)
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

                _logger.LogInformation("Start processing resource '{resourceType}'", resourceType);
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

            _logger.LogInformation("Finish scheduling job '{jobId}'", job.Id);
        }
    }
}
