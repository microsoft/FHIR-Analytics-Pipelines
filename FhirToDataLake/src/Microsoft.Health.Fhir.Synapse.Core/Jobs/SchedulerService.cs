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
using AzureTableTaskQueue.Extensions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.JobManagement;
using NCrontab;
using Newtonsoft.Json;

namespace AzureTableTaskQueue.Synapse
{
    public class SchedulerService
    {
        private TableClient _tableClient;
        private IQueueClient _queueClient;

        private JobConfiguration _jobConfiguration;
        private FhirServerConfiguration _fhirServerConfiguration;
        private DataLakeStoreConfiguration _dataLakeStoreConfiguration;

        // See https://github.com/atifaziz/NCrontab/wiki/Crontab-Expression
        private CrontabSchedule _crontabSchedule;
        private const int PullingIntervalInSeconds = 20;

        public SchedulerService(
            IQueueClient queueClient,
            IOptions<FhirServerConfiguration> fhirServerConfiguration,
            IOptions<DataLakeStoreConfiguration> dataLakeStoreConfiguration,
            IOptions<JobConfiguration> jobConfiguration)
        {
            _fhirServerConfiguration = fhirServerConfiguration.Value;
            _dataLakeStoreConfiguration = dataLakeStoreConfiguration.Value;
            _jobConfiguration = jobConfiguration.Value;

            _crontabSchedule = CrontabSchedule.Parse(_jobConfiguration.SchedulerCronExpression, new CrontabSchedule.ParseOptions { IncludingSeconds = true });
            _tableClient = new TableClient(new Uri(_jobConfiguration.TableUrl), _jobConfiguration.AgentId, new DefaultAzureCredential());
            _tableClient.CreateIfNotExists();
            _queueClient = queueClient;
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var delayTask = Task.Delay(TimeSpan.FromSeconds(PullingIntervalInSeconds));
                var triggerEntity = await GetCurrentTrigger();
                if (triggerEntity == null)
                {
                    // Initial Task
                    triggerEntity = new TriggerIdEntity
                    {
                        PartitionKey = JobTableKeyProvider.TriggerPartitionKey(_jobConfiguration.QueueType),
                        RowKey = JobTableKeyProvider.TriggerPartitionKey(_jobConfiguration.QueueType),
                        TriggerTimeStart = _jobConfiguration.StartTime.DateTime.SetKind(DateTimeKind.Utc),
                        TriggerTimeEnd = DateTime.UtcNow.SetKind(DateTimeKind.Utc),
                        Status = TriggerStatus.New,
                        TriggerId = 1,
                        OrchestratorJobId = 0,
                    };

                    await _tableClient.AddEntityAsync(triggerEntity);
                    triggerEntity = await GetCurrentTrigger();
                }

                if (triggerEntity.Status == TriggerStatus.New)
                {
                    var orchestratorDefinition = new OrchestratorJobInputData
                    {
                        FhirServerUrl = _fhirServerConfiguration.ServerUrl,
                        DataLakeUrl = _dataLakeStoreConfiguration.StorageUrl,
                        ContainerName = _jobConfiguration.AgentId,
                        DataStart = triggerEntity.TriggerTimeStart,
                        DataEnd = triggerEntity.TriggerTimeEnd,
                        ResourceTypes = _jobConfiguration.ResourceTypeFilters,
                        TypeId = (int)JobType.Orchestrator,
                    };

                    var jobInfoList = await _queueClient.EnqueueAsync(_jobConfiguration.QueueType, new string[] { JsonConvert.SerializeObject(orchestratorDefinition) }, triggerEntity.TriggerId, false, false, cancellationToken);
                    triggerEntity.OrchestratorJobId = jobInfoList.First().Id;
                    triggerEntity.Status = TriggerStatus.Running;
                    await _tableClient.UpdateEntityAsync(triggerEntity, triggerEntity.ETag);
                }
                else if (triggerEntity.Status == TriggerStatus.Running)
                {
                    var orchestratorJobId = triggerEntity.OrchestratorJobId;
                    var orchestratorJob = await _queueClient.GetJobByIdAsync(_jobConfiguration.QueueType, orchestratorJobId, false, cancellationToken);
                    if (orchestratorJob.Status == JobStatus.Completed)
                    {
                        triggerEntity.Status = TriggerStatus.Completed;
                    }
                    else if (orchestratorJob.Status == JobStatus.Failed || orchestratorJob.Status == JobStatus.Cancelled)
                    {
                        // ToDo: cancel scheduling when job fails
                        continue;
                    }

                    await _tableClient.UpdateEntityAsync(triggerEntity, triggerEntity.ETag);
                }
                else
                {
                    var nextTriggerTime = _crontabSchedule.GetNextOccurrence(triggerEntity.TriggerTimeEnd);
                    triggerEntity.TriggerTimeStart = triggerEntity.TriggerTimeEnd;
                    triggerEntity.TriggerTimeEnd = nextTriggerTime;
                    triggerEntity.TriggerId += 1;

                    if (nextTriggerTime.AddSeconds(30) < DateTime.UtcNow)
                    {
                        var orchestratorDefinition = new OrchestratorJobInputData
                        {
                            FhirServerUrl = _fhirServerConfiguration.ServerUrl,
                            DataLakeUrl = _dataLakeStoreConfiguration.StorageUrl,
                            ContainerName = _jobConfiguration.AgentId,
                            DataStart = triggerEntity.TriggerTimeStart,
                            DataEnd = triggerEntity.TriggerTimeEnd,
                            ResourceTypes = _jobConfiguration.ResourceTypeFilters,
                            TypeId = (int)JobType.Orchestrator,
                        };
                        var jobInfoList = await _queueClient.EnqueueAsync(_jobConfiguration.QueueType, new string[] { JsonConvert.SerializeObject(orchestratorDefinition) }, triggerEntity.TriggerId, false, false, cancellationToken);
                        triggerEntity.OrchestratorJobId = jobInfoList.First().Id;
                        triggerEntity.Status = TriggerStatus.Running;
                        await _tableClient.UpdateEntityAsync(triggerEntity, triggerEntity.ETag);
                    }
                }

                await delayTask;
            }
        }

        private async Task<TriggerIdEntity> GetCurrentTrigger()
        {
            TriggerIdEntity entity = null;
            try
            {
                var response = await _tableClient.GetEntityAsync<TriggerIdEntity>(JobTableKeyProvider.TriggerPartitionKey(_jobConfiguration.QueueType), JobTableKeyProvider.TriggerPartitionKey(_jobConfiguration.QueueType));
                entity = response.Value;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == "ResourceNotFound")
            {
                Console.WriteLine("Cannot get current trigger. Will create new.");
            }

            return entity;
        }
    }
}
