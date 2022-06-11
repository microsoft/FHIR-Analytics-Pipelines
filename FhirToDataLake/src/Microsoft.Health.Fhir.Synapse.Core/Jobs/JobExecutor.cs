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
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobExecutor
    {
        private readonly ITaskExecutor _taskExecutor;
        private readonly JobProgressUpdaterFactory _jobProgressUpdaterFactory;
        private readonly JobSchedulerConfiguration _schedulerConfiguration;
        private readonly TaskQueue _taskQueue;
        private readonly IFhirDataClient _dataClient;
        private readonly ILogger<JobExecutor> _logger;

        public JobExecutor(
            ITaskExecutor taskExecutor,
            JobProgressUpdaterFactory jobProgressUpdaterFactory,
            IFhirDataClient dataClient,
            IOptions<JobSchedulerConfiguration> schedulerConfiguration,
            TaskQueue taskQueue,
            ILogger<JobExecutor> logger)
        {
            EnsureArg.IsNotNull(taskExecutor, nameof(taskExecutor));
            EnsureArg.IsNotNull(jobProgressUpdaterFactory, nameof(jobProgressUpdaterFactory));
            EnsureArg.IsNotNull(schedulerConfiguration, nameof(schedulerConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _taskExecutor = taskExecutor;
            _jobProgressUpdaterFactory = jobProgressUpdaterFactory;
            _schedulerConfiguration = schedulerConfiguration.Value;
            _dataClient = dataClient;
            _taskQueue = taskQueue;
            _logger = logger;
        }

        public async Task ExecuteAsync(Job job, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start executing job '{jobId}'.", job.Id);

            var jobProgressUpdater = _jobProgressUpdaterFactory.Create(job);
            var progressReportTask = Task.Run(() => jobProgressUpdater.Consume(cancellationToken), cancellationToken);
            var produceTask = ProduceTasks(job);
            var tasks = new List<Task<TaskResult>>();
            var workerCount = 10;
            for (int i = 0; i < workerCount; i += 1)
            {
                tasks.Add(Task.Run(async () => await _taskExecutor.ExecuteAsync(null, jobProgressUpdater, cancellationToken)));

                _logger.LogInformation("Start processing worker '{0}'", i);
            }

            try
            {
                await produceTask;
                Console.WriteLine("All tasks has been produced");
                var taskResults = await Task.WhenAll(tasks);
                Console.WriteLine("Task execution has been produced");
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

        private async Task ProduceTasks(Job job)
        {
            int taskIndex = 0;
            string ct = null;
            do
            {
                var searchParameters = new FhirSearchParameters("Patient", DateTimeOffset.MinValue, DateTimeOffset.MaxValue, ct);
                var fhirBundleResult = await _dataClient.SearchAsync(searchParameters, default);

                // Parse bundle result.
                JObject fhirBundleObject = null;
                try
                {
                    fhirBundleObject = JObject.Parse(fhirBundleResult);
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Failed to parse fhir search result for resource Patient.");
                }

                var fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject);
                var patientIds = fhirResources.Select(r => r["id"].ToString());
                ct = FhirBundleParser.ExtractContinuationToken(fhirBundleObject);
                for (int i = 0; i < patientIds.Count(); i += 20)
                {
                    var context = TaskContext.Create("Patient", job);
                    context.PartId = taskIndex;
                    context.PatientIds = patientIds.Skip(i).Take(20);
                    taskIndex++;

                    await _taskQueue.Enqueue(context);
                    Console.WriteLine($"{DateTime.Now} Enqueued {context.PatientIds.Count()}");
                }

            }
            while (ct != null);

            _taskQueue.Complete();
        }
    }
}
