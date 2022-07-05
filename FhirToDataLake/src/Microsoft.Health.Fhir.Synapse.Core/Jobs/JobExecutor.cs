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
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobExecutor : IJobExecutor
    {
        private readonly ITaskExecutor _taskExecutor;
        private readonly JobProgressUpdaterFactory _jobProgressUpdaterFactory;
        private readonly JobSchedulerConfiguration _schedulerConfiguration;
        private readonly IGroupMemberExtractor _groupMemberExtractor;

        private readonly ILogger<JobExecutor> _logger;

        private const int NumberOfPatientsPerTask = 100;

        public JobExecutor(
            ITaskExecutor taskExecutor,
            JobProgressUpdaterFactory jobProgressUpdaterFactory,
            IGroupMemberExtractor groupMemberExtractor,
            IOptions<JobSchedulerConfiguration> schedulerConfiguration,
            ILogger<JobExecutor> logger)
        {
            EnsureArg.IsNotNull(taskExecutor, nameof(taskExecutor));
            EnsureArg.IsNotNull(jobProgressUpdaterFactory, nameof(jobProgressUpdaterFactory));
            EnsureArg.IsNotNull(groupMemberExtractor, nameof(groupMemberExtractor));
            EnsureArg.IsNotNull(schedulerConfiguration, nameof(schedulerConfiguration));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _taskExecutor = taskExecutor;
            _jobProgressUpdaterFactory = jobProgressUpdaterFactory;
            _groupMemberExtractor = groupMemberExtractor;
            _schedulerConfiguration = schedulerConfiguration.Value;
            _logger = logger;
        }

        public async Task ExecuteAsync(Job job, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start executing job '{jobId}'.", job.Id);

            // extract patient ids from group if they aren't extracted before for group
            if (job.FilterContext.FilterScope == FilterScope.Group && !job.Patients.Any())
            {
                _logger.LogInformation("Start extracting patients from group '{groupId}'.", job.FilterContext.GroupId);

                // For now, the queryParameters is always null.
                // This parameter will be used when we enable filter groups in the future.
                var patientIds = await _groupMemberExtractor.GetGroupPatientsAsync(
                    job.FilterContext.GroupId,
                    null,
                    job.DataPeriod.End,
                    cancellationToken);

                job.Patients = patientIds.Select(patientId => new PatientWrapper(patientId));

                // for processed patient id, set IsNewPatient to false
                // the processed patient ids may be an empty hashset at the beginning, and will be updated when completing a successful job.
                var processedPatientIds = job.FilterContext.ProcessedPatientIds.ToHashSet();
                foreach (var patient in job.Patients.Where(patient => processedPatientIds.Contains(patient.PatientId)))
                {
                    patient.IsNewPatient = false;
                }

                _logger.LogInformation(
                    "Extract {patientCount} patients from group '{groupId}', including {newPatientCount} new patients.",
                    job.Patients.ToHashSet().Count,
                    job.FilterContext.GroupId,
                    job.Patients.Where(p => p.IsNewPatient).ToHashSet().Count);
            }

            var jobProgressUpdater = _jobProgressUpdaterFactory.Create(job);
            var progressReportTask = Task.Run(() => jobProgressUpdater.Consume(cancellationToken), cancellationToken);

            var tasks = new List<Task<TaskResult>>();

            // and running task to task list for resume job
            tasks.AddRange(job.RunningTasks.Values.Select(taskContext => Task.Run(async () =>
                await _taskExecutor.ExecuteAsync(taskContext, jobProgressUpdater, cancellationToken))).ToList());

            while (true)
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

                TaskContext taskContext = null;

                switch (job.FilterContext.FilterScope)
                {
                    case FilterScope.System:
                        if (job.NextTaskIndex < job.FilterContext.TypeFilters.Count())
                        {
                            var typeFilters = new List<TypeFilter>
                                { job.FilterContext.TypeFilters.ToList()[job.NextTaskIndex] };
                            taskContext = new TaskContext(
                                Guid.NewGuid().ToString("N"),
                                job.NextTaskIndex,
                                job.Id,
                                job.FilterContext.FilterScope,
                                job.DataPeriod,
                                job.FilterContext.Since,
                                typeFilters);
                        }

                        break;
                    case FilterScope.Group:
                        if (job.NextTaskIndex * NumberOfPatientsPerTask < job.Patients.ToList().Count)
                        {
                            var selectedPatients = job.Patients.Skip(job.NextTaskIndex * NumberOfPatientsPerTask)
                                .Take(NumberOfPatientsPerTask);
                            taskContext = new TaskContext(
                                Guid.NewGuid().ToString("N"),
                                job.NextTaskIndex,
                                job.Id,
                                job.FilterContext.FilterScope,
                                job.DataPeriod,
                                job.FilterContext.Since,
                                job.FilterContext.TypeFilters.ToList(),
                                selectedPatients);
                        }

                        break;
                    default:
                        // this case should not happen
                        throw new ArgumentOutOfRangeException($"The filterScope {job.FilterContext.FilterScope} isn't supported now.");
                }

                // if there is no new task context created, then all the tasks are generated, break the while loop;
                if (taskContext == null)
                {
                    break;
                }

                job.RunningTasks[taskContext.Id] = taskContext;
                tasks.Add(Task.Run(async () => await _taskExecutor.ExecuteAsync(taskContext, jobProgressUpdater, cancellationToken)));
                _logger.LogInformation("Start processing task '{taskIndex}'", job.NextTaskIndex);
                job.NextTaskIndex++;
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
