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
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;
using Newtonsoft.Json;
using Xunit;
using JobStatus = Microsoft.Health.JobManagement.JobStatus;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class SchedulerServiceTests
    {
        private readonly NullLogger<SchedulerService> _nullSchedulerServiceLogger =
            NullLogger<SchedulerService>.Instance;

        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";
        private const string TestAgentName = "testAgentName";
        private const string TestWorkerName = "test-worker";
        private const string TestSchedulerCronExpression = "*/2 * * * * *";

        private readonly IOptions<JobConfiguration> _jobConfigOption;
        private IAzureTableClientFactory _azureTableClientFactory;
        private TableClient _metaDataTableClient;

        public SchedulerServiceTests()
        {
            var jobConfig = new JobConfiguration
            {
                QueueType = QueueType.FhirToDataLake,
                TableUrl = StorageEmulatorConnectionString,
                SchedulerCronExpression = TestSchedulerCronExpression,
                AgentName = TestAgentName,
            };

            _jobConfigOption = Options.Create(jobConfig);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new SchedulerService(null, _azureTableClientFactory, Options.Create(new JobConfiguration()), _nullSchedulerServiceLogger));
        }

        [Fact]
        public void GivenInvalidConfiguration_WhenInitialize_ExceptionShouldBeThrown()
        {
            var jobConfig = new JobConfiguration
            {
                QueueType = QueueType.FhirToDataLake,
                TableUrl = StorageEmulatorConnectionString,
                SchedulerCronExpression = "invalid cron expression",
                AgentName = TestAgentName,
            };

            var uniqueName = Guid.NewGuid().ToString("N");
            var agentName = $"agent{uniqueName}";

            // Make sure the container is deleted before running the tests
            _azureTableClientFactory = new AzureTableClientFactory(
                TableKeyProvider.MetadataTableName(agentName),
                new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()));

            var queueClient = new MockQueueClient();

            Assert.Throws<NCrontab.CrontabException>(
                () => new SchedulerService(queueClient, _azureTableClientFactory, Options.Create(jobConfig), _nullSchedulerServiceLogger));
        }

        [Fact]
        public async Task GivenValidParameters_WhenInitialize_ThenTheSchedulerServiceShouldBeCreated()
        {
            try
            {
                var uniqueName = Guid.NewGuid().ToString("N");
                var agentName = $"agent{uniqueName}";

                // Make sure the container is deleted before running the tests
                _azureTableClientFactory = new AzureTableClientFactory(
                    TableKeyProvider.MetadataTableName(agentName),
                    new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()));

                _metaDataTableClient = _azureTableClientFactory.Create();

                var queueClient = new MockQueueClient();

                _ = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption,
                    _nullSchedulerServiceLogger);

                // the table is already created, so will return null.
                Assert.Null(await _metaDataTableClient.CreateIfNotExistsAsync());
            }
            finally
            {
                await CleanStorage();
            }
        }

        [Fact]
        public async Task GivenQueueClientNotInitialized_WhenRunAsync_ThenTheTriggerShouldNotBeEnqueued()
        {
            await InitializeUniqueStorage();

            try
            {
                var queueClient = new MockQueueClient
                {
                    Initialized = false,
                };
                var schedulerService = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption,
                    _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                };

                // the trigger lease entity should not exist
                var exception =
                    await Assert.ThrowsAsync<RequestFailedException>(async () => await GetTriggerLeaseEntity());

                Assert.Equal("ResourceNotFound", exception.ErrorCode);

                using var tokenSource = new CancellationTokenSource();
                await schedulerService.RunAsync(tokenSource.Token);

                // the trigger lease entity should exist
                var triggerLeaseEntity = await GetTriggerLeaseEntity();
                Assert.NotNull(triggerLeaseEntity);

                // the trigger entity should not exist
                exception = await Assert.ThrowsAsync<RequestFailedException>(
                    async () => await GetCurrentTriggerEntity());

                Assert.Equal("ResourceNotFound", exception.ErrorCode);
            }
            finally
            {
                await CleanStorage();
            }
        }

        [Fact]
        public async Task GivenValidTrigger_WhenRunAsync_ThenTheTriggerShouldBeProcessed()
        {
            await InitializeUniqueStorage();

            try
            {
                var queueClient = new MockQueueClient();

                var schedulerService = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                };

                using var tokenSource = new CancellationTokenSource();
                var task = schedulerService.RunAsync(tokenSource.Token);

                await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);

                // should enqueue orchestrator job
                var currentTriggerEntity = await GetCurrentTriggerEntity();
                Assert.NotNull(currentTriggerEntity);

                Assert.Equal(1, currentTriggerEntity.OrchestratorJobId);
                Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
                Assert.Null(currentTriggerEntity.TriggerStartTime);

                var lastTriggerEndTime = currentTriggerEntity.TriggerEndTime;

                // the job is dequeued, and the trigger status is still running.
                var jobInfo = await queueClient.DequeueAsync(
                    (byte)QueueType.FhirToDataLake,
                    TestWorkerName,
                    0,
                    CancellationToken.None);

                await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);

                currentTriggerEntity = await GetCurrentTriggerEntity();
                Assert.NotNull(currentTriggerEntity);

                Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

                // job is completed, the trigger status should be set to next trigger
                jobInfo.Status = JobStatus.Completed;
                await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

                // current trigger entity is running
                await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);
                currentTriggerEntity = await GetCurrentTriggerEntity();
                Assert.NotNull(currentTriggerEntity);
                Assert.Equal(2, currentTriggerEntity.OrchestratorJobId);
                Assert.Equal(1, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
                Assert.Equal(lastTriggerEndTime, currentTriggerEntity.TriggerStartTime);

                // the next job is created
                jobInfo =
                    await queueClient.GetJobByIdAsync((byte)QueueType.FhirToDataLake, 2, true, CancellationToken.None);
                Assert.Equal(JobStatus.Created, jobInfo.Status);

                tokenSource.Cancel();
                await task;
            }
            finally
            {
                await CleanStorage();
            }
        }

        [Fact]
        public async Task GivenLongRunningSchedulerService_WhenRunAsync_ThenTheLeaseShouldBeRenewed()
        {
            await InitializeUniqueStorage();
            try
            {
                var queueClient = new MockQueueClient();

                var schedulerService = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                };

                using var tokenSource = new CancellationTokenSource();
                var task = schedulerService.RunAsync(tokenSource.Token);
                await Task.Delay(TimeSpan.FromMilliseconds(100));
                var triggerLeaseEntity = await GetTriggerLeaseEntity();
                Assert.NotNull(triggerLeaseEntity);

                await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

                var newTriggerLeaseEntity = await GetTriggerLeaseEntity();
                Assert.NotNull(newTriggerLeaseEntity);

                // the lease is renewed.
                Assert.Equal(triggerLeaseEntity.WorkingInstanceGuid, newTriggerLeaseEntity.WorkingInstanceGuid);
                Assert.True(newTriggerLeaseEntity.HeartbeatDateTime > triggerLeaseEntity.HeartbeatDateTime);

                tokenSource.Cancel();
                await task;
            }
            finally
            {
                await CleanStorage();
            }
        }

        [Fact]
        public async Task GivenCrashSchedulerService_WhenRunAsync_ThenTheLeaseShouldBeAcquiredByOtherSchedulerService()
        {
            await InitializeUniqueStorage();
            try
            {
                var queueClient = new MockQueueClient();

                var schedulerService1 = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

                var schedulerService2 = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

                // service 1 is running
                using var tokenSource1 = new CancellationTokenSource();
                var task1 = schedulerService1.RunAsync(tokenSource1.Token);
                await Task.Delay(TimeSpan.FromMilliseconds(100));

                // service 1 acquire lease
                var triggerLeaseEntity = await GetTriggerLeaseEntity();
                Assert.NotNull(triggerLeaseEntity);
                var workingGuid1 = triggerLeaseEntity.WorkingInstanceGuid;

                // service 2 is running
                using var tokenSource2 = new CancellationTokenSource();
                var task2 = schedulerService2.RunAsync(tokenSource2.Token);
                await Task.Delay(TimeSpan.FromMilliseconds(100));

                // scheduler service 2 can't acquire lease
                triggerLeaseEntity = await GetTriggerLeaseEntity();
                Assert.Equal(workingGuid1, triggerLeaseEntity.WorkingInstanceGuid);

                // service is stopped
                tokenSource1.Cancel();
                await task1;

                await Task.Delay(TimeSpan.FromSeconds(6));

                // scheduler service 2 should acquire lease
                triggerLeaseEntity = await GetTriggerLeaseEntity();
                Assert.NotEqual(workingGuid1, triggerLeaseEntity.WorkingInstanceGuid);

                tokenSource2.Cancel();
                await task2;
            }
            finally
            {
                await CleanStorage();
            }
        }

        [Fact]
        public async Task GivenRunningTrigger_WhenReRunAsync_ThenTheTriggerShouldBePickedUp()
        {
            await InitializeUniqueStorage();
            try
            {
                var queueClient = new MockQueueClient();

                var schedulerService = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

                // service is running
                using var tokenSource1 = new CancellationTokenSource();
                var task1 = schedulerService.RunAsync(tokenSource1.Token);

                // should enqueue orchestrator job
                CurrentTriggerEntity currentTriggerEntity = null;
                while (currentTriggerEntity == null)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);

                    currentTriggerEntity = await GetCurrentTriggerEntity();
                }

                Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
                Assert.Null(currentTriggerEntity.TriggerStartTime);

                // the job is dequeued
                await Task.Delay(TimeSpan.FromSeconds(2));
                var jobInfo = await queueClient.DequeueAsync(
                    (byte)QueueType.FhirToDataLake,
                    TestWorkerName,
                    0,
                    CancellationToken.None);

                // job is completed, the trigger status should be set to next trigger
                jobInfo.Status = JobStatus.Completed;
                await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

                await Task.Delay(TimeSpan.FromSeconds(10));

                tokenSource1.Cancel();
                await task1;

                // task1 stops, the trigger status is running, trigger sequence id is 1.
                currentTriggerEntity = await GetCurrentTriggerEntity();
                Assert.NotNull(currentTriggerEntity);
                Assert.Equal(1, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

                // re-run task2
                using var tokenSource2 = new CancellationTokenSource();
                var task2 = schedulerService.RunAsync(tokenSource2.Token);

                await Task.Delay(TimeSpan.FromMilliseconds(100));

                // resume trigger
                currentTriggerEntity = await GetCurrentTriggerEntity();
                Assert.NotNull(currentTriggerEntity);
                Assert.Equal(1, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

                jobInfo = await queueClient.DequeueAsync(
                    (byte)QueueType.FhirToDataLake,
                    TestWorkerName,
                    0,
                    CancellationToken.None);

                // job is completed, the trigger status should be set to next trigger
                jobInfo.Status = JobStatus.Completed;
                await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

                await Task.Delay(TimeSpan.FromSeconds(5));

                currentTriggerEntity = await GetCurrentTriggerEntity();
                Assert.NotNull(currentTriggerEntity);
                Assert.Equal(2, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

                tokenSource2.Cancel();
                await task2;
            }
            finally
            {
                await CleanStorage();
            }
        }

        [Fact]
        public async Task GivenFailedTrigger_WhenReRunAsync_ThenTheTriggerShouldNotBeResumed()
        {
            await InitializeUniqueStorage();
            try
            {
                var queueClient = new MockQueueClient();

                var schedulerService = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

                // service is running
                using var tokenSource1 = new CancellationTokenSource();
                var task1 = schedulerService.RunAsync(tokenSource1.Token);
                await Task.Delay(TimeSpan.FromMilliseconds(100));

                // the job is dequeued
                var jobInfo = await queueClient.DequeueAsync(
                    (byte)QueueType.FhirToDataLake,
                    TestWorkerName,
                    0,
                    CancellationToken.None);

                // job is completed, the trigger status should be set to next trigger
                jobInfo.Status = JobStatus.Failed;
                await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

                // the scheduler service should stop
                await task1;

                var currentTriggerEntity = await GetCurrentTriggerEntity();
                Assert.NotNull(currentTriggerEntity);

                // current trigger entity is failed
                Assert.Equal(jobInfo.Id, currentTriggerEntity.OrchestratorJobId);
                Assert.Equal(TriggerStatus.Failed, currentTriggerEntity.TriggerStatus);
                var triggerLeaseEntity = await GetTriggerLeaseEntity();

                var task2 = schedulerService.RunAsync(tokenSource1.Token);

                await task2;

                // the currentTriggerEntity should not be updated
                var currentTriggerEntity2 = await GetCurrentTriggerEntity();
                Assert.NotNull(currentTriggerEntity2);

                // current trigger entity is failed
                Assert.Equal(jobInfo.Id, currentTriggerEntity2.OrchestratorJobId);
                Assert.Equal(TriggerStatus.Failed, currentTriggerEntity2.TriggerStatus);
                Assert.Equal(currentTriggerEntity.Timestamp, currentTriggerEntity2.Timestamp);

                // lease should be renewed
                var triggerLeaseEntity2 = await GetTriggerLeaseEntity();
                Assert.Equal(triggerLeaseEntity.WorkingInstanceGuid, triggerLeaseEntity2.WorkingInstanceGuid);
                Assert.True(triggerLeaseEntity2.HeartbeatDateTime > triggerLeaseEntity.HeartbeatDateTime);
            }
            finally
            {
                await CleanStorage();
            }
        }

        [Fact]
        public async Task GivenEnqueueFailure_WhenRunAsync_ThenTheTriggerShouldBeRetried()
        {
            await InitializeUniqueStorage();
            try
            {
                var brokenQueueClient = new MockQueueClient();

                void FaultAction() => throw new RequestFailedException("fake request failed exception");

                brokenQueueClient.EnqueueFaultAction = FaultAction;

                var schedulerService1 = new SchedulerService(brokenQueueClient, _azureTableClientFactory, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

                // service is running
                using var tokenSource1 = new CancellationTokenSource();
                var task1 = schedulerService1.RunAsync(tokenSource1.Token);

                await Task.Delay(TimeSpan.FromMilliseconds(100));

                // the current trigger status is new.
                var currentTriggerEntity = await GetCurrentTriggerEntity();

                Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.New, currentTriggerEntity.TriggerStatus);
                Assert.Equal(0, currentTriggerEntity.OrchestratorJobId);

                tokenSource1.Cancel();
                await task1;

                var queueClient = new MockQueueClient();
                var schedulerService2 = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

                using var tokenSource2 = new CancellationTokenSource();
                var task2 = schedulerService2.RunAsync(tokenSource2.Token);

                // wait for service 2 acquires lease
                await Task.Delay(TimeSpan.FromSeconds(5));

                // the current trigger status is running.
                currentTriggerEntity = await GetCurrentTriggerEntity();

                Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
                Assert.Equal(1, currentTriggerEntity.OrchestratorJobId);

                tokenSource2.Cancel();
                await task2;
            }
            finally
            {
                await CleanStorage();
            }
        }

        [Fact]
        public async Task GivenReEnqueueJob_WhenRunAsync_ThenTheExistingJobShouldBeReturned()
        {
            await InitializeUniqueStorage();
            try
            {
                var queueClient = new MockQueueClient();

                var initialTriggerEntity = new CurrentTriggerEntity
                {
                    PartitionKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.FhirToDataLake),
                    RowKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.FhirToDataLake),
                    TriggerStartTime = null,
                    TriggerEndTime = DateTime.UtcNow,
                    TriggerStatus = TriggerStatus.New,
                    TriggerSequenceId = 0,
                };

                await _metaDataTableClient.CreateIfNotExistsAsync();

                // add the initial trigger entity to table
                await _metaDataTableClient.AddEntityAsync(initialTriggerEntity, CancellationToken.None);

                // enqueue manually
                var orchestratorDefinition1 = new FhirToDataLakeOrchestratorJobInputData
                {
                    JobType = JobType.Orchestrator,
                    DataStartTime = initialTriggerEntity.TriggerStartTime,
                    DataEndTime = initialTriggerEntity.TriggerEndTime,
                };

                var jobInfoList = (await queueClient.EnqueueAsync(
                    (byte)QueueType.FhirToDataLake,
                    new[] { JsonConvert.SerializeObject(orchestratorDefinition1) },
                    0,
                    false,
                    false,
                    CancellationToken.None)).ToList();

                var schedulerService = new SchedulerService(queueClient, _azureTableClientFactory, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

                // service is running
                using var tokenSource = new CancellationTokenSource();
                var task = schedulerService.RunAsync(tokenSource.Token);

                await Task.Delay(TimeSpan.FromMilliseconds(100));

                var currentTriggerEntity = await GetCurrentTriggerEntity();
                Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
                Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
                Assert.Equal(jobInfoList.First().Id, currentTriggerEntity.OrchestratorJobId);

                var jobInfo = await queueClient.GetJobByIdAsync((byte)QueueType.FhirToDataLake, currentTriggerEntity.OrchestratorJobId, true, CancellationToken.None);

                Assert.Equal(jobInfoList.First().HeartbeatDateTime, jobInfo.HeartbeatDateTime);

                tokenSource.Cancel();
                await task;
            }
            finally
            {
                await CleanStorage();
            }
        }

        private async Task InitializeUniqueStorage()
        {
            var uniqueName = Guid.NewGuid().ToString("N");
            var agentName = $"agent{uniqueName}";

            // Make sure the container is deleted before running the tests
            _azureTableClientFactory = new AzureTableClientFactory(
                TableKeyProvider.MetadataTableName(agentName),
                new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()));

            _metaDataTableClient = _azureTableClientFactory.Create();
            await _metaDataTableClient.CreateIfNotExistsAsync();
        }

        private async Task CleanStorage()
        {
            await _metaDataTableClient.DeleteAsync();
        }

        private async Task<CurrentTriggerEntity> GetCurrentTriggerEntity() =>
            (await _metaDataTableClient.GetEntityAsync<CurrentTriggerEntity>(
                TableKeyProvider.TriggerPartitionKey((byte)QueueType.FhirToDataLake),
                TableKeyProvider.TriggerRowKey((byte)QueueType.FhirToDataLake),
                cancellationToken: CancellationToken.None)).Value;

        private async Task<TriggerLeaseEntity> GetTriggerLeaseEntity() =>
            (await _metaDataTableClient.GetEntityAsync<TriggerLeaseEntity>(
                TableKeyProvider.LeasePartitionKey((byte)QueueType.FhirToDataLake),
                TableKeyProvider.LeaseRowKey((byte)QueueType.FhirToDataLake),
                cancellationToken: CancellationToken.None)).Value;
    }
}
