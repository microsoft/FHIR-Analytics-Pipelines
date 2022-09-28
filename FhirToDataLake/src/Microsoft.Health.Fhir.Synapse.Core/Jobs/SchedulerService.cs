// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.Fhir.Synapse.JobManagement;
using Microsoft.Health.JobManagement;
using NCrontab;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class SchedulerService : ISchedulerService
    {
        private readonly IQueueClient _queueClient;

        private readonly IMetadataStore _metadataStore;
        private readonly byte _queueType;
        private readonly ILogger<SchedulerService> _logger;
        private readonly Guid _instanceGuid;
        private readonly JobConfiguration _jobConfiguration;
        private readonly IDiagnosticLogger _diagnosticLogger;

        // See https://github.com/atifaziz/NCrontab/wiki/Crontab-Expression
        private readonly CrontabSchedule _crontabSchedule;

        public SchedulerService(
            IQueueClient queueClient,
            IMetadataStore metadataStore,
            IOptions<JobConfiguration> jobConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<SchedulerService> logger)
        {
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _jobConfiguration = jobConfiguration.Value;
            _queueType = (byte)jobConfiguration.Value.QueueType;
            _crontabSchedule = CrontabSchedule.Parse(jobConfiguration.Value.SchedulerCronExpression, new CrontabSchedule.ParseOptions { IncludingSeconds = true });

            _metadataStore = EnsureArg.IsNotNull(metadataStore, nameof(metadataStore));
            _instanceGuid = Guid.NewGuid();
        }

        public DateTimeOffset LastHeartbeat { get; set; } = DateTimeOffset.UtcNow;

        public int SchedulerServicePullingIntervalInSeconds { get; set; } = JobConfigurationConstants.DefaultSchedulerServicePullingIntervalInSeconds;

        public int SchedulerServiceLeaseExpirationInSeconds { get; set; } = JobConfigurationConstants.DefaultSchedulerServiceLeaseExpirationInSeconds;

        public int SchedulerServiceLeaseRefreshIntervalInSeconds { get; set; } = JobConfigurationConstants.DefaultSchedulerServiceLeaseRefreshIntervalInSeconds;

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            _diagnosticLogger.LogInformation("Scheduler starts running.");
            _logger.LogInformation("Scheduler starts running.");
            using CancellationTokenSource cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            CancellationToken serviceCancellationToken = cancellationTokenSource.Token;
            var stopRunning = false;

            while (!stopRunning && !serviceCancellationToken.IsCancellationRequested)
            {
                var delayTask = Task.Delay(TimeSpan.FromSeconds(SchedulerServicePullingIntervalInSeconds), CancellationToken.None);

                try
                {
                    // try next time if the queue client isn't initialized.
                    if (!_queueClient.IsInitialized() || !_metadataStore.IsInitialized())
                    {
                        _diagnosticLogger.LogInformation("The storage isn't initialized.");
                        _logger.LogInformation("The storage isn't initialized.");
                    }
                    else
                    {
                        var leaseAcquired = await TryAcquireLeaseAsync(serviceCancellationToken);

                        if (leaseAcquired)
                        {
                            try
                            {
                                var renewLeaseTask = RenewLeaseAsync(serviceCancellationToken);

                                // we don't catch exceptions in this function, if the request is cancelled, it could return false or throw exception
                                // we should stop renew lease for both cases.
                                var processTriggerTask = TryPullAndProcessTriggerEntity(cancellationTokenSource);
                                await Task.WhenAll(renewLeaseTask, processTriggerTask);
                                stopRunning = processTriggerTask.Result;
                            }
                            catch (Exception ex)
                            {
                                _diagnosticLogger.LogWarning("Failed to pull and update trigger.");
                                _logger.LogInformation(ex, "Failed to pull and update trigger.");
                            }
                        }
                    }

                    await delayTask;
                    LastHeartbeat = DateTimeOffset.UtcNow;
                }
                catch (Exception ex)
                {
                    _diagnosticLogger.LogWarning($"There is an exception thrown while processing current trigger, will retry later. Exception {ex.Message};");
                    _logger.LogInformation($"There is an exception thrown while processing current trigger, will retry later. Exception {ex.Message};");
                }
            }

            _logger.LogInformation("Scheduler stops running.");
        }

        private async Task<bool> TryAcquireLeaseAsync(CancellationToken cancellationToken)
        {
            try
            {
                TriggerLeaseEntity triggerLeaseEntity;
                var needRenewLease = true;
                try
                {
                    // try to get trigger lease entity
                    triggerLeaseEntity = await _metadataStore.GetTriggerLeaseEntityAsync(_queueType, cancellationToken);
                }
                catch (RequestFailedException ex) when (ex.ErrorCode == AzureStorageErrorCode.GetEntityNotFoundErrorCode)
                {
                    await CreateInitialTriggerLeaseEntityAsync(cancellationToken);

                    // try to get trigger lease entity after insert
                    triggerLeaseEntity = await _metadataStore.GetTriggerLeaseEntityAsync(_queueType, cancellationToken);
                    needRenewLease = false;
                }

                if (triggerLeaseEntity == null)
                {
                    _diagnosticLogger.LogInformation("Try to acquire lease failed, the retrieved lease trigger entity is null.");
                    _logger.LogInformation("Try to acquire lease failed, the retrieved lease trigger entity is null.");
                    return false;
                }

                if (triggerLeaseEntity.WorkingInstanceGuid != _instanceGuid &&
                    triggerLeaseEntity.HeartbeatDateTime.AddSeconds(SchedulerServiceLeaseExpirationInSeconds) > DateTimeOffset.UtcNow)
                {
                    _diagnosticLogger.LogInformation("Try to acquire lease failed, another worker is using it.");
                    _logger.LogInformation("Try to acquire lease failed, another worker is using it.");
                    return false;
                }

                if (needRenewLease)
                {
                    triggerLeaseEntity.WorkingInstanceGuid = _instanceGuid;
                    triggerLeaseEntity.HeartbeatDateTime = DateTimeOffset.UtcNow;

                    await _metadataStore.UpdateEntityAsync(triggerLeaseEntity, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogInformation(
                    $"Try acquire lease failed. Exception: {ex.Message}.");
                _logger.LogInformation(
                    $"Try acquire lease failed. Exception: {ex.Message}.");
                return false;
            }

            _diagnosticLogger.LogInformation("Acquire lease successfully.");
            _logger.LogInformation("Acquire lease successfully.");
            return true;
        }

        private async Task CreateInitialTriggerLeaseEntityAsync(CancellationToken cancellationToken)
        {
            var initialTriggerLeaseEntity = new TriggerLeaseEntity()
            {
                PartitionKey = TableKeyProvider.LeasePartitionKey(_queueType),
                RowKey = TableKeyProvider.LeaseRowKey(_queueType),
                WorkingInstanceGuid = _instanceGuid,
                HeartbeatDateTime = DateTimeOffset.UtcNow,
            };

            // add the initial trigger entity to table
            await _metadataStore.AddEntityAsync(initialTriggerLeaseEntity, cancellationToken);

            _diagnosticLogger.LogInformation("Create initial trigger lease entity successfully.");
            _logger.LogInformation("Create initial trigger lease entity successfully.");
        }

        private async Task<bool> TryPullAndProcessTriggerEntity(CancellationTokenSource cancellationTokenSource)
        {
            var cancellationToken = cancellationTokenSource.Token;
            while (!cancellationToken.IsCancellationRequested)
            {
                var intervalDelayTask = Task.Delay(TimeSpan.FromSeconds(SchedulerServicePullingIntervalInSeconds), CancellationToken.None);

                var currentTriggerEntity = await _metadataStore.GetCurrentTriggerEntityAsync(_queueType, cancellationToken) ??
                                           await CreateInitialTriggerEntity(cancellationToken);

                // if current trigger entity is still null after initializing, just skip processing it this time and try again next time.
                // this case should not happen.
                if (currentTriggerEntity != null)
                {
                    switch (currentTriggerEntity.TriggerStatus)
                    {
                        case TriggerStatus.New:
                            await EnqueueOrchestratorJobAsync(currentTriggerEntity, cancellationToken);
                            break;
                        case TriggerStatus.Running:
                            await CheckAndUpdateOrchestratorJobStatusAsync(currentTriggerEntity, cancellationToken);
                            break;
                        case TriggerStatus.Completed:
                            await CreateNextTriggerAsync(currentTriggerEntity, cancellationToken);
                            break;
                        case TriggerStatus.Failed:
                        case TriggerStatus.Cancelled:
                            // if the current trigger is cancelled/failed, stop scheduler, this case should not happen
                            _diagnosticLogger.LogWarning("Trigger Status is cancelled or failed.");
                            _logger.LogInformation("Trigger Status is cancelled or failed.");
                            cancellationTokenSource.Cancel();
                            return true;
                    }
                }

                await intervalDelayTask;
                LastHeartbeat = DateTimeOffset.UtcNow;
            }

            return false;
        }

        private async Task EnqueueOrchestratorJobAsync(CurrentTriggerEntity currentTriggerEntity, CancellationToken cancellationToken)
        {
            // enqueue a orchestrator job for this trigger
            var orchestratorDefinition = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = currentTriggerEntity.TriggerSequenceId,
                Since = _jobConfiguration?.StartTime,
                DataStartTime = currentTriggerEntity.TriggerStartTime,
                DataEndTime = currentTriggerEntity.TriggerEndTime,
            };

            var jobInfoList = await _queueClient.EnqueueAsync(
                _queueType,
                new[] { JsonConvert.SerializeObject(orchestratorDefinition) },
                currentTriggerEntity.TriggerSequenceId,
                false,
                false,
                cancellationToken);

            currentTriggerEntity.OrchestratorJobId = jobInfoList.First().Id;
            currentTriggerEntity.TriggerStatus = TriggerStatus.Running;
            await _metadataStore.UpdateEntityAsync(currentTriggerEntity, cancellationToken);

            _diagnosticLogger.LogInformation(
                $"Enqueue orchestrator job for trigger index {currentTriggerEntity.TriggerSequenceId}, the orchestrator job id is {currentTriggerEntity.OrchestratorJobId}.");
            _logger.LogInformation(
                $"Enqueue orchestrator job for trigger index {currentTriggerEntity.TriggerSequenceId}, the orchestrator job id is {currentTriggerEntity.OrchestratorJobId}.");
        }

        private async Task CheckAndUpdateOrchestratorJobStatusAsync(CurrentTriggerEntity currentTriggerEntity, CancellationToken cancellationToken)
        {
            var orchestratorJobId = currentTriggerEntity.OrchestratorJobId;
            var orchestratorJob = await _queueClient.GetJobByIdAsync(
                _queueType,
                orchestratorJobId,
                false,
                cancellationToken);

            var needUpdateTriggerEntity = true;

            // job status "Archived" is useless,
            // shouldn't set job status to archived, if do, need to update the handle logic here
            switch (orchestratorJob.Status)
            {
                case JobStatus.Completed:
                    currentTriggerEntity.TriggerStatus = TriggerStatus.Completed;
                    _diagnosticLogger.LogInformation("Current trigger is completed.");
                    _logger.LogInformation("Current trigger is completed.");
                    break;
                case JobStatus.Failed:
                    currentTriggerEntity.TriggerStatus = TriggerStatus.Failed;
                    _diagnosticLogger.LogWarning("The orchestrator job is failed.");
                    _logger.LogInformation("The orchestrator job is failed.");
                    break;
                case JobStatus.Cancelled:
                    currentTriggerEntity.TriggerStatus = TriggerStatus.Cancelled;
                    _diagnosticLogger.LogWarning("The orchestrator job is cancelled.");
                    _logger.LogInformation("The orchestrator job is cancelled.");
                    break;
                default:
                    needUpdateTriggerEntity = false;
                    break;
            }

            if (needUpdateTriggerEntity)
            {
                await _metadataStore.UpdateEntityAsync(currentTriggerEntity, cancellationToken);
            }
        }

        private async Task CreateNextTriggerAsync(CurrentTriggerEntity currentTriggerEntity, CancellationToken cancellationToken)
        {
            var nextTriggerDateTimeTime =
                _crontabSchedule.GetNextOccurrence(currentTriggerEntity.TriggerEndTime.DateTime);

            var nextTriggerTime = DateTime.SpecifyKind(nextTriggerDateTimeTime, DateTimeKind.Utc);

            // next trigger time was the past time, assign the fields of next trigger to the currentTriggerEntity
            if (nextTriggerTime.AddMinutes(JobConfigurationConstants.JobQueryLatencyInMinutes) < DateTimeOffset.UtcNow)
            {
                currentTriggerEntity.TriggerSequenceId += 1;
                currentTriggerEntity.TriggerStatus = TriggerStatus.New;

                currentTriggerEntity.TriggerStartTime = GetTriggerStartTime(currentTriggerEntity.TriggerEndTime);
                currentTriggerEntity.TriggerEndTime = GetTriggerEndTime(nextTriggerTime);
                if (currentTriggerEntity.TriggerStartTime > currentTriggerEntity.TriggerEndTime)
                {
                    _diagnosticLogger.LogInformation("The job has been scheduled to end.");
                    _logger.LogInformation("The job has been scheduled to end.");
                    return;
                }

                await _metadataStore.UpdateEntityAsync(currentTriggerEntity, cancellationToken);

                _diagnosticLogger.LogInformation(
                    $"A new trigger with sequence id {currentTriggerEntity.TriggerSequenceId} is updated to azure table.");
                _logger.LogInformation(
                    $"A new trigger with sequence id {currentTriggerEntity.TriggerSequenceId} is updated to azure table.");
            }
        }

        /// <summary>
        /// Create initial trigger entity
        /// </summary>
        /// <param name="cancellationToken">cancellation token</param>
        /// <returns>The created trigger entity</returns>
        private async Task<CurrentTriggerEntity> CreateInitialTriggerEntity(CancellationToken cancellationToken)
        {
            var initialTriggerEntity = new CurrentTriggerEntity
            {
                PartitionKey = TableKeyProvider.TriggerPartitionKey(_queueType),
                RowKey = TableKeyProvider.TriggerPartitionKey(_queueType),
                TriggerStartTime = GetTriggerStartTime(null),
                TriggerEndTime = GetTriggerEndTime(null),
                TriggerStatus = TriggerStatus.New,
                TriggerSequenceId = 0,
            };

            try
            {
                // add the initial trigger entity to table
                await _metadataStore.AddEntityAsync(initialTriggerEntity, cancellationToken);
            }
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogWarning($"Failed to add trigger entity to table, exception: {ex.Message}");
                _logger.LogInformation($"Failed to add trigger entity to table, exception: {ex.Message}");
                throw;
            }

            return await _metadataStore.GetCurrentTriggerEntityAsync(_queueType, cancellationToken);
        }

        private DateTimeOffset? GetTriggerStartTime(DateTimeOffset? lastTriggerEndTime)
        {
            return lastTriggerEndTime ?? _jobConfiguration?.StartTime;
        }

        // Job end time could be null (which means runs forever) or a timestamp in the future like 2120/01/01.
        // In this case, we will create a job to run with end time earlier that current timestamp.
        // Also, FHIR data use processing time as lastUpdated timestamp, there might be some latency when saving to data store.
        // Here we add a JobEndTimeLatencyInMinutes latency to avoid data missing due to latency in creation.
        private DateTimeOffset GetTriggerEndTime(DateTimeOffset? occurrenceTime)
        {
            // Add latency here to allow latency in saving resources to database.
            var endTime = occurrenceTime ?? DateTimeOffset.UtcNow.AddMinutes(-1 * JobConfigurationConstants.JobQueryLatencyInMinutes);

            if (_jobConfiguration?.EndTime != null && endTime > _jobConfiguration.EndTime)
            {
                endTime = (DateTimeOffset)_jobConfiguration.EndTime;
            }

            return endTime;
        }

        private async Task RenewLeaseAsync(CancellationToken cancellationToken)
        {
            _diagnosticLogger.LogInformation($"Start to renew lease for working instance {_instanceGuid}.");
            _logger.LogInformation($"Start to renew lease for working instance {_instanceGuid}.");

            while (!cancellationToken.IsCancellationRequested)
            {
                var intervalDelayTask = Task.Delay(TimeSpan.FromSeconds(SchedulerServiceLeaseRefreshIntervalInSeconds), CancellationToken.None);

                try
                {
                    // try to get trigger lease entity
                    var triggerLeaseEntity =
                        await _metadataStore.GetTriggerLeaseEntityAsync(_queueType, cancellationToken);

                    if (triggerLeaseEntity == null || triggerLeaseEntity.WorkingInstanceGuid != _instanceGuid)
                    {
                        _diagnosticLogger.LogWarning("Failed to renew lease, the retrieved lease trigger entity is null or working instance doesn't match.");
                        _logger.LogInformation("Failed to renew lease, the retrieved lease trigger entity is null or working instance doesn't match.");
                    }
                    else
                    {
                        triggerLeaseEntity.HeartbeatDateTime = DateTimeOffset.UtcNow;

                        await _metadataStore.UpdateEntityAsync(
                            triggerLeaseEntity,
                            cancellationToken: cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    _diagnosticLogger.LogWarning($"Failed to renew lease for working instance {_instanceGuid}.");
                    _logger.LogInformation(ex, $"Failed to renew lease for working instance {_instanceGuid}.");
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    await intervalDelayTask;
                }
            }

            _diagnosticLogger.LogInformation($"Stop to renew lease for working instance {_instanceGuid}.");
            _logger.LogInformation($"Stop to renew lease for working instance {_instanceGuid}.");
        }
    }
}