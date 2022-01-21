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
        private readonly ITaskExecutor _taskExecutor;
        private readonly JobProgressUpdaterFactory _jobProgressUpdaterFactory;
        private readonly JobSchedulerConfiguration _schedulerConfiguration;
        private readonly ILogger<JobExecutor> _logger;

        public JobExecutor(
            ITaskExecutor taskExecutor,
            JobProgressUpdaterFactory jobProgressUpdaterFactory,
            IOptions<JobSchedulerConfiguration> schedulerConfiguration,
            ILogger<JobExecutor> logger)
        {
            EnsureArg.IsNotNull(taskExecutor, nameof(taskExecutor));
            EnsureArg.IsNotNull(jobProgressUpdaterFactory, nameof(jobProgressUpdaterFactory));
            EnsureArg.IsNotNull(schedulerConfiguration, nameof(schedulerConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _taskExecutor = taskExecutor;
            _jobProgressUpdaterFactory = jobProgressUpdaterFactory;
            _schedulerConfiguration = schedulerConfiguration.Value;
            _logger = logger;
        }

        public async Task ExecuteAsync(Job job, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start executing job '{jobId}'.", job.Id);

            var jobProgressUpdater = _jobProgressUpdaterFactory.Create(job);
            var progressReportTask = Task.Run(() => jobProgressUpdater.Consume(cancellationToken), cancellationToken);

            var tasks = new List<Task<TaskResult>>();
            foreach (var resourceType in job.ResourceTypes)
            {
                if (tasks.Count >= _schedulerConfiguration.MaxConcurrencyCount)
                {
                    var finishedTask = await Task.WhenAny(tasks);
                    if (finishedTask.IsFaulted)
                    {
                        _logger.LogError(finishedTask.Exception, "Process task failed.");
                        throw new ExecuteTaskFailedException("Task execution failed", finishedTask.Exception);
                    }

                    tasks.Remove(finishedTask);
                }

                var context = TaskContext.Create(resourceType, job);
                if (context.IsCompleted)
                {
                    _logger.LogInformation("Skipping completed resource '{resourceType}'.", resourceType);
                    continue;
                }

                tasks.Add(Task.Run(async () => await _taskExecutor.ExecuteAsync(context, jobProgressUpdater, cancellationToken)));

                _logger.LogInformation("Start processing resource '{resourceType}'", resourceType);
            }

            try
            {
                var taskResults = await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Process task failed.");
                throw new ExecuteTaskFailedException("Task execution failed", ex);
            }

            jobProgressUpdater.Complete();
            await progressReportTask;

            _logger.LogInformation("Finish scheduling job '{jobId}'", job.Id);
        }
    }
}
