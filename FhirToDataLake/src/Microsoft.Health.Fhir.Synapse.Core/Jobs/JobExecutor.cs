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

        public JobExecutor(
            ITaskExecutor taskExecutor,
            JobProgressUpdaterFactory jobProgressUpdaterFactory,
            IGroupMemberExtractor groupMemberExtractor,
            IOptions<JobSchedulerConfiguration> schedulerConfiguration,
            ILogger<JobExecutor> logger)
        {
            EnsureArg.IsNotNull(schedulerConfiguration, nameof(schedulerConfiguration));
            _schedulerConfiguration = schedulerConfiguration.Value;

            _taskExecutor = EnsureArg.IsNotNull(taskExecutor, nameof(taskExecutor));
            _jobProgressUpdaterFactory = EnsureArg.IsNotNull(jobProgressUpdaterFactory, nameof(jobProgressUpdaterFactory));
            _groupMemberExtractor = EnsureArg.IsNotNull(groupMemberExtractor, nameof(groupMemberExtractor));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger)); ;
        }

        public async Task ExecuteAsync(Job job, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Start executing job '{jobId}'.", job.Id);

            // extract patient ids from group if they aren't extracted before for group
            if (job.FilterInfo.FilterScope == FilterScope.Group && !job.Patients.Any())
            {
                _logger.LogInformation("Start extracting patients from group '{groupId}'.", job.FilterInfo.GroupId);

                // For now, the queryParameters is always null.
                // This parameter will be used when we enable filter groups in the future.
                var patientIds = await _groupMemberExtractor.GetGroupPatientsAsync(
                    job.FilterInfo.GroupId,
                    null,
                    job.DataPeriod.End,
                    cancellationToken);

                // set the version id for processed patient
                // the processed patients is empty at the beginning , and will be updated when completing a successful job.
                job.Patients = patientIds.Select(patientId =>
                    new PatientWrapper(
                        patientId,
                        job.FilterInfo.ProcessedPatients.ContainsKey(patientId) ? job.FilterInfo.ProcessedPatients[patientId] : 0));

                _logger.LogInformation(
                    "Extract {patientCount} patients from group '{groupId}', including {newPatientCount} new patients.",
                    patientIds.Count,
                    job.FilterInfo.GroupId,
                    job.Patients.Where(p => p.VersionId == 0).ToList().Count);
            }

            var jobProgressUpdater = _jobProgressUpdaterFactory.Create(job);
            var progressReportTask = Task.Run(() => jobProgressUpdater.Consume(cancellationToken), cancellationToken);

            var tasks = new List<Task<TaskResult>>();

            try
            {
                // add running task to task list for resume job
                tasks.AddRange(job.RunningTasks.Values.Select(taskContext => Task.Run(async () =>
                    await _taskExecutor.ExecuteAsync(taskContext, jobProgressUpdater, cancellationToken))).ToList());

                var filters = job.FilterInfo.TypeFilters.ToList();
                while (true)
                {
                    if (tasks.Count >= _schedulerConfiguration.MaxConcurrencyCount)
                    {
                        var finishedTask = await Task.WhenAny(tasks);
                        tasks.Remove(finishedTask);

                        if (finishedTask.IsFaulted)
                        {
                            _logger.LogError(finishedTask.Exception, "Process task failed.");

                            // if there is a task failed, we need to wait all the task to be completed
                            // otherwise, the data in other tasks may be missing 
                            await Task.WhenAll(tasks);

                            throw new ExecuteTaskFailedException("Task execution failed", finishedTask.Exception);
                        }
                    }

                    TaskContext taskContext = null;

                    switch (job.FilterInfo.FilterScope)
                    {
                        case FilterScope.System:
                            if (job.NextTaskIndex < job.FilterInfo.TypeFilters.Count())
                            {
                                var typeFilters = new List<TypeFilter>
                                    { filters[job.NextTaskIndex] };
                                taskContext = TaskContext.CreateFromJob(job, typeFilters);
                            }

                            break;
                        case FilterScope.Group:
                            if (job.NextTaskIndex * JobConfigurationConstants.NumberOfPatientsPerTask < job.Patients.ToList().Count)
                            {
                                var selectedPatients = job.Patients.Skip(job.NextTaskIndex * JobConfigurationConstants.NumberOfPatientsPerTask)
                                    .Take(JobConfigurationConstants.NumberOfPatientsPerTask).ToList();
                                taskContext = TaskContext.CreateFromJob(job, filters, selectedPatients);

                            }

                            break;
                        default:
                            // this case should not happen
                            throw new ArgumentOutOfRangeException($"The filterScope {job.FilterInfo.FilterScope} isn't supported now.");
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

                var taskResults = await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Process task failed.");
                throw new ExecuteTaskFailedException("Task execution failed", ex);
            }
            finally
            {
                // if there is a task failed, we need to consume all the task contexts before exit jobExecutor
                // otherwise, the task contexts may be consumed and update the job to active after the job is completed in JobManager
                jobProgressUpdater.Complete();
                await progressReportTask;
            }

            _logger.LogInformation("Finish scheduling job '{jobId}'", job.Id);
        }
    }
}
