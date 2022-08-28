// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
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

        private readonly TableClient _metaDataTableClient;
        private readonly byte _queueType;
        private readonly ILogger<SchedulerService> _logger;
        private readonly Guid _instanceGuid;
        private readonly JobConfiguration _jobConfiguration;

        // See https://github.com/atifaziz/NCrontab/wiki/Crontab-Expression
        private readonly CrontabSchedule _crontabSchedule;

        public SchedulerService(
            IQueueClient queueClient,
            IAzureTableClientFactory azureTableClientFactory,
            IOptions<JobConfiguration> jobConfiguration,
            ILogger<SchedulerService> logger)
        {
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            EnsureArg.IsNotNull(azureTableClientFactory, nameof(azureTableClientFactory));
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _jobConfiguration = jobConfiguration.Value;
            _queueType = (byte)jobConfiguration.Value.QueueType;
            _crontabSchedule = CrontabSchedule.Parse(jobConfiguration.Value.SchedulerCronExpression, new CrontabSchedule.ParseOptions { IncludingSeconds = true });
            _metaDataTableClient = azureTableClientFactory.Create();
            _metaDataTableClient.CreateIfNotExists();
            _instanceGuid = Guid.NewGuid();
        }

        public int SchedulerServicePullingIntervalInSeconds { get; set; } = JobConfigurationConstants.DefaultSchedulerServicePullingIntervalInSeconds;

        public int SchedulerServiceLeaseExpirationInSeconds { get; set; } = JobConfigurationConstants.DefaultSchedulerServiceLeaseExpirationInSeconds;

        public int SchedulerServiceLeaseRefreshIntervalInSeconds { get; set; } = JobConfigurationConstants.DefaultSchedulerServiceLeaseRefreshIntervalInSeconds;

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Scheduler starts running.");

            var stopRunning = false;

            while (!stopRunning && !cancellationToken.IsCancellationRequested)
            {
                var delayTask = Task.Delay(TimeSpan.FromSeconds(SchedulerServicePullingIntervalInSeconds), CancellationToken.None);

                try
                {
                    var leaseAcquired = await TryAcquireLeaseAsync(cancellationToken);

                    if (leaseAcquired)
                    {
                        using var renewLeaseCancellationToken = new CancellationTokenSource();
                        var renewLeaseTask = RenewLeaseAsync(renewLeaseCancellationToken.Token);
                        try
                        {
                            // we don't catch exceptions in this function, if the request is cancelled, it could return false or throw exception
                            // we should stop renew lease for both cases.
                            stopRunning = await TryPullAndProcessTriggerEntity(cancellationToken);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Failed to pull and update trigger.");
                        }

                        renewLeaseCancellationToken.Cancel();
                        await renewLeaseTask;
                    }

                    await delayTask;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"There is an exception thrown while processing current trigger, will retry later. Exception {ex.Message};");
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
                    triggerLeaseEntity = (await _metaDataTableClient.GetEntityAsync<TriggerLeaseEntity>(
                        TableKeyProvider.LeasePartitionKey(_queueType),
                        TableKeyProvider.LeaseRowKey(_queueType),
                        cancellationToken: cancellationToken)).Value;
                }
                catch (RequestFailedException ex) when (ex.ErrorCode == AzureStorageErrorCode.GetEntityNotFoundErrorCode)
                {
                    _logger.LogWarning("The current trigger lease doesn't exist, will create a new one.");

                    var initialTriggerLeaseEntity = new TriggerLeaseEntity()
                    {
                        PartitionKey = TableKeyProvider.LeasePartitionKey(_queueType),
                        RowKey = TableKeyProvider.LeaseRowKey(_queueType),
                        WorkingInstanceGuid = _instanceGuid,
                        HeartbeatDateTime = DateTimeOffset.UtcNow,
                    };

                    // add the initial trigger entity to table
                    await _metaDataTableClient.AddEntityAsync(initialTriggerLeaseEntity, cancellationToken);

                    // try to get trigger lease entity after insert
                    triggerLeaseEntity = (await _metaDataTableClient.GetEntityAsync<TriggerLeaseEntity>(
                        TableKeyProvider.LeasePartitionKey(_queueType),
                        TableKeyProvider.LeaseRowKey(_queueType),
                        cancellationToken: cancellationToken)).Value;
                    needRenewLease = false;
                }

                if (triggerLeaseEntity == null)
                {
                    _logger.LogWarning("Try to acquire lease failed, the retrieved lease trigger entity is null.");
                    return false;
                }

                if (triggerLeaseEntity.WorkingInstanceGuid != _instanceGuid &&
                    triggerLeaseEntity.HeartbeatDateTime.AddSeconds(SchedulerServiceLeaseExpirationInSeconds) > DateTimeOffset.UtcNow)
                {
                    _logger.LogInformation("Try to acquire lease failed, another worker is using it.");
                    return false;
                }

                if (needRenewLease)
                {
                    triggerLeaseEntity.WorkingInstanceGuid = _instanceGuid;
                    triggerLeaseEntity.HeartbeatDateTime = DateTimeOffset.UtcNow;

                    await _metaDataTableClient.UpdateEntityAsync(
                        triggerLeaseEntity,
                        triggerLeaseEntity.ETag,
                        cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    $"Try acquire lease failed. Exception: {ex.Message}.");
                return false;
            }

            _logger.LogInformation("Acquire lease successfully.");
            return true;
        }

        private async Task<bool> TryPullAndProcessTriggerEntity(CancellationToken cancellationToken)
        {
            // try next time if the queue client isn't initialized.
            if (!_queueClient.IsInitialized())
            {
                _logger.LogWarning("Try to acquire lease failed, the queue client isn't initialized.");
                return true;
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                var intervalDelayTask = Task.Delay(TimeSpan.FromSeconds(SchedulerServicePullingIntervalInSeconds), CancellationToken.None);

                var currentTriggerEntity = await GetCurrentTriggerEntity(cancellationToken) ??
                                           await CreateInitialTriggerEntity(cancellationToken);

                // if current trigger entity is still null after initializing, just skip processing it this time and try again next time.
                // this case should not happen.
                if (currentTriggerEntity != null)
                {
                    switch (currentTriggerEntity.TriggerStatus)
                    {
                        case TriggerStatus.New:
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
                                await _metaDataTableClient.UpdateEntityAsync(
                                    currentTriggerEntity,
                                    currentTriggerEntity.ETag,
                                    cancellationToken: cancellationToken);

                                _logger.LogInformation(
                                    $"Enqueue orchestrator job for trigger index {currentTriggerEntity.TriggerSequenceId}, the orchestrator job id is {currentTriggerEntity.OrchestratorJobId}.");
                                break;
                            }

                        case TriggerStatus.Running:
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
                                        _logger.LogInformation("Current trigger is completed.");
                                        break;
                                    case JobStatus.Failed:
                                        currentTriggerEntity.TriggerStatus = TriggerStatus.Failed;
                                        _logger.LogError("The orchestrator job is failed.");
                                        break;
                                    case JobStatus.Cancelled:
                                        currentTriggerEntity.TriggerStatus = TriggerStatus.Cancelled;
                                        _logger.LogError("The orchestrator job is cancelled.");
                                        break;
                                    default:
                                        needUpdateTriggerEntity = false;
                                        break;
                                }

                                if (needUpdateTriggerEntity)
                                {
                                    await _metaDataTableClient.UpdateEntityAsync(
                                        currentTriggerEntity,
                                        currentTriggerEntity.ETag,
                                        cancellationToken: cancellationToken);
                                }

                                break;
                            }

                        case TriggerStatus.Completed:
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
                                        _logger.LogInformation("The job has been scheduled to end.");
                                        return true;
                                    }

                                    await _metaDataTableClient.UpdateEntityAsync(
                                        currentTriggerEntity,
                                        currentTriggerEntity.ETag,
                                        cancellationToken: cancellationToken);

                                    _logger.LogInformation(
                                        $"A new trigger with sequence id {currentTriggerEntity.TriggerSequenceId} is updated to azure table.");
                                }

                                break;
                            }

                        case TriggerStatus.Failed:

                        case TriggerStatus.Cancelled:
                            // if the current trigger is cancelled/failed, stop scheduler, this case should not happen
                            _logger.LogWarning("Trigger Status is cancelled or failed.");
                            return true;
                    }
                }

                await intervalDelayTask;
            }

            return false;
        }

        /// <summary>
        /// Get current trigger entity from azure table
        /// </summary>
        /// <param name="cancellationToken">cancellation token</param>
        /// <returns>Current trigger entity, return null if does not exist.</returns>
        private async Task<CurrentTriggerEntity> GetCurrentTriggerEntity(CancellationToken cancellationToken)
        {
            CurrentTriggerEntity entity = null;
            try
            {
                var response = await _metaDataTableClient.GetEntityAsync<CurrentTriggerEntity>(
                    TableKeyProvider.TriggerPartitionKey(_queueType),
                    TableKeyProvider.TriggerRowKey(_queueType),
                    cancellationToken: cancellationToken);
                entity = response.Value;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == AzureStorageErrorCode.GetEntityNotFoundErrorCode)
            {
                _logger.LogWarning("The current trigger doesn't exist, will create a new one.");
            }
            catch (Exception ex)
            {
                // any exceptions while getting entity will log a error and try next time
                _logger.LogError($"Failed to get current trigger entity from table, exception: {ex.Message}");
                throw;
            }

            return entity;
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
                await _metaDataTableClient.AddEntityAsync(initialTriggerEntity, cancellationToken);
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError($"Failed to add trigger entity to table, exception: {ex.Message}");
                throw;
            }

            return await GetCurrentTriggerEntity(cancellationToken);
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
            _logger.LogInformation($"Start to renew lease for working instance {_instanceGuid}.");

            while (!cancellationToken.IsCancellationRequested)
            {
                var intervalDelayTask = Task.Delay(TimeSpan.FromSeconds(SchedulerServiceLeaseRefreshIntervalInSeconds), CancellationToken.None);

                try
                {
                    // try to get trigger lease entity
                    var triggerLeaseEntity = (await _metaDataTableClient.GetEntityAsync<TriggerLeaseEntity>(
                        TableKeyProvider.LeasePartitionKey(_queueType),
                        TableKeyProvider.LeaseRowKey(_queueType),
                        cancellationToken: cancellationToken)).Value;

                    if (triggerLeaseEntity == null || triggerLeaseEntity.WorkingInstanceGuid != _instanceGuid)
                    {
                        _logger.LogWarning("Failed to renew lease, the retrieved lease trigger entity is null or working instance doesn't match.");
                    }
                    else
                    {
                        triggerLeaseEntity.HeartbeatDateTime = DateTimeOffset.UtcNow;

                        await _metaDataTableClient.UpdateEntityAsync(
                            triggerLeaseEntity,
                            triggerLeaseEntity.ETag,
                            cancellationToken: cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Failed to renew lease for working instance {_instanceGuid}.");
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    await intervalDelayTask;
                }
            }

            _logger.LogInformation($"Stop to renew lease for working instance {_instanceGuid}.");
        }
    }
}