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
using Azure.Identity;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.JobManagement;
using NCrontab;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class SchedulerService : ISchedulerService
    {
        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";

        private readonly IQueueClient _queueClient;

        // TODO: a tableClient provider component?
        private readonly TableClient _metaDataTableClient;

        private readonly byte _queueType;

        private readonly ILogger<SchedulerService> _logger;
        private readonly Guid _instanceGuid;

        // See https://github.com/atifaziz/NCrontab/wiki/Crontab-Expression
        private readonly CrontabSchedule _crontabSchedule;

        public SchedulerService(
            IQueueClient queueClient,
            IOptions<JobConfiguration> jobConfiguration,
            ILogger<SchedulerService> logger)
        {
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            _queueType = (byte)jobConfiguration.Value.QueueType;

            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _crontabSchedule = CrontabSchedule.Parse(jobConfiguration.Value.SchedulerCronExpression, new CrontabSchedule.ParseOptions { IncludingSeconds = true });

            _instanceGuid = Guid.NewGuid();

            // Create client for local emulator.
            if (string.Equals(jobConfiguration.Value.TableUrl, StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                _metaDataTableClient = new TableClient(
                    jobConfiguration.Value.TableUrl,
                    JobKeyProvider.MetadataTableName(jobConfiguration.Value.AgentName));
            }
            else
            {
                _metaDataTableClient = new TableClient(
                    new Uri(jobConfiguration.Value.TableUrl),
                    JobKeyProvider.MetadataTableName(jobConfiguration.Value.AgentName),
                    new DefaultAzureCredential());
            }

            _metaDataTableClient.CreateIfNotExists();
        }

        public int PullingIntervalInSeconds { get; set; } = JobConfigurationConstants.DefaultPullingIntervalInSeconds;

        public int HeartbeatTimeoutInSeconds { get; set; } = JobConfigurationConstants.DefaultHeartbeatTimeoutInSeconds;

        public int HeartbeatIntervalInSeconds { get; set; } = JobConfigurationConstants.DefaultHeartbeatIntervalInSeconds;

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Scheduler starts running.");

            var stopRunning = false;

            // TODO: exception handling for azure table/queue exceptions
            while (!stopRunning && !cancellationToken.IsCancellationRequested)
            {
                var delayTask = Task.Delay(TimeSpan.FromSeconds(PullingIntervalInSeconds), CancellationToken.None);

                try
                {
                    var leaseAcquired = await TryAcquireLeaseAsync(cancellationToken);

                    if (leaseAcquired)
                    {
                        using var renewLeaseCancellationToken = new CancellationTokenSource();
                        var renewLeaseTask = RenewLeaseAsync(renewLeaseCancellationToken.Token);

                        stopRunning = await TryPullAndProcessTriggerEntity(cancellationToken);

                        renewLeaseCancellationToken.Cancel();
                        await renewLeaseTask;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning($"There is an exception thrown while processing current trigger, will retry later. Exception {ex.Message};");
                }

                await delayTask;
            }

            _logger.LogInformation("Scheduler stops running.");
        }

        private async Task<bool> TryAcquireLeaseAsync(CancellationToken cancellationToken)
        {
            try
            {
                TriggerLeaseEntity triggerLeaseEntity;
                try
                {
                    // try to get trigger lease entity
                    triggerLeaseEntity = (await _metaDataTableClient.GetEntityAsync<TriggerLeaseEntity>(
                        JobKeyProvider.LeasePartitionKey(_queueType),
                        JobKeyProvider.LeaseRowKey(_queueType),
                        cancellationToken: cancellationToken)).Value;
                }
                catch (RequestFailedException ex) when (ex.ErrorCode == "ResourceNotFound")
                {
                    _logger.LogWarning("The current trigger lease doesn't exist, will create a new one.");

                    var initialTriggerLeaseEntity = new TriggerLeaseEntity()
                    {
                        PartitionKey = JobKeyProvider.LeasePartitionKey(_queueType),
                        RowKey = JobKeyProvider.LeaseRowKey(_queueType),
                        WorkingInstanceGuid = _instanceGuid,
                        HeartbeatDateTime = DateTime.UtcNow,
                    };

                    // add the initial trigger entity to table
                    await _metaDataTableClient.AddEntityAsync(initialTriggerLeaseEntity, cancellationToken);

                    // try to get trigger lease entity after insert
                    triggerLeaseEntity = (await _metaDataTableClient.GetEntityAsync<TriggerLeaseEntity>(
                        JobKeyProvider.LeasePartitionKey(_queueType),
                        JobKeyProvider.LeaseRowKey(_queueType),
                        cancellationToken: cancellationToken)).Value;
                }

                if (triggerLeaseEntity == null)
                {
                    _logger.LogWarning("Try to acquire lease failed, the retrieved lease trigger entity is null.");
                    return false;
                }

                if (triggerLeaseEntity.WorkingInstanceGuid != _instanceGuid &&
                    triggerLeaseEntity.HeartbeatDateTime.AddSeconds(HeartbeatTimeoutInSeconds) > DateTimeOffset.UtcNow)
                {
                    _logger.LogInformation("Try to acquire lease failed, another worker is using it.");
                    return false;
                }

                triggerLeaseEntity.WorkingInstanceGuid = _instanceGuid;
                triggerLeaseEntity.HeartbeatDateTime = DateTime.UtcNow;

                await _metaDataTableClient.UpdateEntityAsync(
                    triggerLeaseEntity,
                    triggerLeaseEntity.ETag,
                    cancellationToken: cancellationToken);
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
                    JobKeyProvider.TriggerPartitionKey(_queueType),
                    JobKeyProvider.TriggerRowKey(_queueType),
                    cancellationToken: cancellationToken);
                entity = response.Value;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "ResourceNotFound")
            {
                _logger.LogWarning("The current trigger doesn't exist, will create a new one.");
            }
            catch (Exception ex)
            {
                // TODO: double check it can catch RequestFailedException and ArgumentNullException
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
                PartitionKey = JobKeyProvider.TriggerPartitionKey(_queueType),
                RowKey = JobKeyProvider.TriggerPartitionKey(_queueType),
                TriggerStartTime = null,

                // TODO: the trigger end time may be different for different instances?
                TriggerEndTime = GetTriggerEndTime(DateTimeOffset.UtcNow),
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

        // FHIR data use processing time as lastUpdated timestamp, there might be some latency when saving to data store.
        // Here we add a JobQueryLatencyInMinutes latency to avoid data missing due to latency in creation.
        private static DateTime GetTriggerEndTime(DateTimeOffset occurrenceTime)
        {
            // Add latency here to allow latency in saving resources to database.
            var endTime = occurrenceTime.AddMinutes(-1 * AzureBlobJobConstants.JobQueryLatencyInMinutes);

            return DateTime.SpecifyKind(endTime.DateTime, DateTimeKind.Utc);
        }

        // should make sure this function will no throw exception
        private async Task<bool> TryPullAndProcessTriggerEntity(CancellationToken cancellationToken)
        {
            try
            {
                // try next time if the queue client isn't initialized.
                if (!_queueClient.IsInitialized())
                {
                    _logger.LogWarning("Try to acquire lease failed, the queue client isn't initialized.");
                    return true;
                }

                while (!cancellationToken.IsCancellationRequested)
                {
                    var intervalDelayTask = Task.Delay(TimeSpan.FromSeconds(PullingIntervalInSeconds), CancellationToken.None);

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
                                        DataStartTime = currentTriggerEntity.TriggerStartTime,
                                        DataEndTime = currentTriggerEntity.TriggerEndTime,
                                    };

                                    // TODO: need to handle the case: multi same orchestrator jobs are enqueue by different agent instances
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

                                    // TODO: how to handle job status is Archived
                                    switch (orchestratorJob.Status)
                                    {
                                        // TODO: There should not throw exceptions in job executor
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
                                    var nextTriggerTime =
                                        _crontabSchedule.GetNextOccurrence(currentTriggerEntity.TriggerEndTime);

                                    // next trigger time was the past time, assign the fields of next trigger to the currentTriggerEntity
                                    if (nextTriggerTime < DateTime.UtcNow)
                                    {
                                        currentTriggerEntity.TriggerSequenceId += 1;
                                        currentTriggerEntity.TriggerStatus = TriggerStatus.New;

                                        currentTriggerEntity.TriggerStartTime = currentTriggerEntity.TriggerEndTime;
                                        currentTriggerEntity.TriggerEndTime = GetTriggerEndTime(nextTriggerTime);

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
                                // if the current trigger is cancelled/failed, stop scheduler.
                                _logger.LogWarning("Trigger Status is cancelled or failed.");
                                return true;
                        }
                    }

                    await intervalDelayTask;
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to pull and update trigger.");
            }

            return false;
        }

        private async Task RenewLeaseAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start to renew lease for working instance {_instanceGuid}.");

            while (!cancellationToken.IsCancellationRequested)
            {
                var intervalDelayTask = Task.Delay(TimeSpan.FromSeconds(HeartbeatIntervalInSeconds), CancellationToken.None);

                try
                {
                    // try to get trigger lease entity
                    var triggerLeaseEntity = (await _metaDataTableClient.GetEntityAsync<TriggerLeaseEntity>(
                        JobKeyProvider.LeasePartitionKey(_queueType),
                        JobKeyProvider.LeaseRowKey(_queueType),
                        cancellationToken: cancellationToken)).Value;

                    if (triggerLeaseEntity == null || triggerLeaseEntity.WorkingInstanceGuid != _instanceGuid)
                    {
                        _logger.LogWarning("Failed to renew lease, the retrieved lease trigger entity is null or working instance doesn't match.");
                    }
                    else
                    {
                        triggerLeaseEntity.HeartbeatDateTime = DateTime.UtcNow;

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

                await intervalDelayTask;
            }

            _logger.LogInformation($"Stop to renew lease for working instance {_instanceGuid}.");
        }
    }
}