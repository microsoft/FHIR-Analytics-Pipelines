// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;
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
        private const string TestSchedulerCronExpression = "5 * * * * *";

        private readonly IOptions<JobConfiguration> _jobConfigOption;
        private readonly TableClient _metaDataTableClient;

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

            _metaDataTableClient = new TableClient(
                StorageEmulatorConnectionString,
                JobKeyProvider.MetadataTableName(TestAgentName));

        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new SchedulerService(null, Options.Create(new JobConfiguration()), _nullSchedulerServiceLogger));
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

            var queueClient = new MockQueueClient();

            Assert.Throws<NCrontab.CrontabException>(
                () => new SchedulerService(queueClient, Options.Create(jobConfig), _nullSchedulerServiceLogger));
        }

        [Fact]
        public async Task GivenValidParameters_WhenInitialize_ThenTheSchedulerServiceShouldBeCreated()
        {
            // delete table
            await _metaDataTableClient.DeleteAsync(CancellationToken.None);

            var queueClient = new MockQueueClient();

            _ = new SchedulerService(queueClient, _jobConfigOption, _nullSchedulerServiceLogger);

            // the table is already created, so will return null.
            Assert.Null(await _metaDataTableClient.CreateIfNotExistsAsync());
        }

        [Fact]
        public async Task GivenQueueClientNotInitialized_WhenRunAsync_ThenTheTriggerShouldNotBeEnqueued()
        {
            // delete table
            await _metaDataTableClient.DeleteAsync(CancellationToken.None);

            var queueClient = new MockQueueClient
            {
                Initialized = false,
            };
            var schedulerService = new SchedulerService(queueClient, _jobConfigOption, _nullSchedulerServiceLogger)
                {
                    PullingIntervalInSeconds = 0,
                    HeartbeatIntervalInSeconds = 1,
                };

            // the trigger lease entity should not exist
            var exception = await Assert.ThrowsAsync<RequestFailedException>(async () => await GetTriggerLeaseEntity());

            Assert.Equal("ResourceNotFound", exception.ErrorCode);

            using var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(TimeSpan.FromSeconds(5));

            await schedulerService.RunAsync(tokenSource.Token);

            // the trigger lease entity should exist
            var triggerLeaseEntity = await GetTriggerLeaseEntity();
            Assert.NotNull(triggerLeaseEntity);

            // the trigger entity should not exist
            exception = await Assert.ThrowsAsync<RequestFailedException>(async () => await GetCurrentTriggerEntity());

            Assert.Equal("ResourceNotFound", exception.ErrorCode);
        }

        [Fact]
        public async Task GivenValidTrigger_WhenRunAsync_ThenTheTriggerShouldBeProcessed()
        {
            // delete table
            await _metaDataTableClient.DeleteAsync(CancellationToken.None);

            var queueClient = new MockQueueClient();

            var schedulerService = new SchedulerService(queueClient, _jobConfigOption, _nullSchedulerServiceLogger)
            {
                PullingIntervalInSeconds = 0,
                HeartbeatIntervalInSeconds = 1,
            };

            using var tokenSource = new CancellationTokenSource();
            var task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromSeconds(1));

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

            await Task.Delay(TimeSpan.FromSeconds(1));

            currentTriggerEntity = await GetCurrentTriggerEntity();
            Assert.NotNull(currentTriggerEntity);

            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            // job is completed, the trigger status should be set to next trigger
            jobInfo.Status = JobStatus.Completed;
            await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

            await Task.Delay(TimeSpan.FromSeconds(1));
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

            // the job is dequeued, and the trigger status is still running.
            var jobInfo2 = await queueClient.DequeueAsync(
                (byte)QueueType.FhirToDataLake,
                TestWorkerName,
                0,
                CancellationToken.None);

            // job is failed
            jobInfo2.Status = JobStatus.Failed;
            await queueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);

            // the scheduler service should stop
            await task;

            currentTriggerEntity = await GetCurrentTriggerEntity();
            Assert.NotNull(currentTriggerEntity);

            // current trigger entity is failed
            Assert.Equal(jobInfo2.Id, currentTriggerEntity.OrchestratorJobId);
            Assert.Equal(TriggerStatus.Failed, currentTriggerEntity.TriggerStatus);
        }

        [Fact]
        public async Task GivenLongRunningSchedulerService_WhenRunAsync_ThenTheLeaseShouldBeRenewed()
        {
            await _metaDataTableClient.DeleteAsync();
            var queueClient = new MockQueueClient();

            var schedulerService = new SchedulerService(queueClient, _jobConfigOption, _nullSchedulerServiceLogger)
            {
                PullingIntervalInSeconds = 0,
                HeartbeatIntervalInSeconds = 1,
            };

            using var tokenSource = new CancellationTokenSource();
            var task = schedulerService.RunAsync(tokenSource.Token);
            await Task.Delay(TimeSpan.FromSeconds(1));
            var triggerLeaseEntity = await GetTriggerLeaseEntity();
            Assert.NotNull(triggerLeaseEntity);

            await Task.Delay(TimeSpan.FromSeconds(2));

            var newTriggerLeaseEntity = await GetTriggerLeaseEntity();
            Assert.NotNull(newTriggerLeaseEntity);

            // the lease is renewed.
            Assert.Equal(triggerLeaseEntity.WorkingInstanceGuid, newTriggerLeaseEntity.WorkingInstanceGuid);
            Assert.True(newTriggerLeaseEntity.HeartbeatDateTime > triggerLeaseEntity.HeartbeatDateTime);

            tokenSource.Cancel();
            await task;
        }

        [Fact]
        public async Task GivenCrashSchedulerService_WhenRunAsync_ThenTheLeaseShouldBeAcquiredByOtherSchedulerService()
        {
            await _metaDataTableClient.DeleteAsync();

            var queueClient = new MockQueueClient();

            var schedulerService1 = new SchedulerService(queueClient, _jobConfigOption, _nullSchedulerServiceLogger)
            {
                PullingIntervalInSeconds = 0,
                HeartbeatIntervalInSeconds = 1,
                HeartbeatTimeoutInSeconds = 2,
            };

            var schedulerService2 = new SchedulerService(queueClient, _jobConfigOption, _nullSchedulerServiceLogger)
            {
                PullingIntervalInSeconds = 0,
                HeartbeatIntervalInSeconds = 1,
                HeartbeatTimeoutInSeconds = 2,
            };

            // service 1 is running
            using var tokenSource1 = new CancellationTokenSource();
            var task1 = schedulerService1.RunAsync(tokenSource1.Token);
            await Task.Delay(TimeSpan.FromSeconds(1));

            // service 1 acquire lease
            var triggerLeaseEntity = await GetTriggerLeaseEntity();
            Assert.NotNull(triggerLeaseEntity);
            var workingGuid1 = triggerLeaseEntity.WorkingInstanceGuid;

            // service 2 is running
            using var tokenSource2 = new CancellationTokenSource();
            var task2 = schedulerService2.RunAsync(tokenSource2.Token);
            await Task.Delay(TimeSpan.FromSeconds(1));

            // scheduler service 2 can't acquire lease
            triggerLeaseEntity = await GetTriggerLeaseEntity();
            Assert.Equal(workingGuid1, triggerLeaseEntity.WorkingInstanceGuid);

            // service is stopped
            tokenSource1.Cancel();
            await task1;

            await Task.Delay(TimeSpan.FromSeconds(5));

            // scheduler service 2 should acquire lease
            triggerLeaseEntity = await GetTriggerLeaseEntity();
            Assert.NotEqual(workingGuid1, triggerLeaseEntity.WorkingInstanceGuid);

            tokenSource2.Cancel();
            await task2;
        }

        [Fact]
        public async Task GivenRunningTrigger_WhenReRunAsync_ThenTheTriggerShouldBePickedUp()
        {
            await _metaDataTableClient.DeleteAsync();

            var queueClient = new MockQueueClient();

            var schedulerService = new SchedulerService(queueClient, _jobConfigOption, _nullSchedulerServiceLogger)
            {
                PullingIntervalInSeconds = 0,
                HeartbeatIntervalInSeconds = 1,
                HeartbeatTimeoutInSeconds = 2,
            };

            // service is running
            using var tokenSource1 = new CancellationTokenSource();
            tokenSource1.CancelAfter(TimeSpan.FromSeconds(5));
            var task1 = schedulerService.RunAsync(tokenSource1.Token);
            await Task.Delay(TimeSpan.FromSeconds(1));

            // the job is dequeued
            var jobInfo = await queueClient.DequeueAsync(
                (byte)QueueType.FhirToDataLake,
                TestWorkerName,
                0,
                CancellationToken.None);

            // job is completed, the trigger status should be set to next trigger
            jobInfo.Status = JobStatus.Completed;
            await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

            await task1;

            // task1 stops, the trigger status is running, trigger sequence id is 1.
            var currentTriggerEntity = await GetCurrentTriggerEntity();
            Assert.NotNull(currentTriggerEntity);
            Assert.Equal(1, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            // re-run task2
            using var tokenSource2 = new CancellationTokenSource();
            var task2 = schedulerService.RunAsync(tokenSource2.Token);

            await Task.Delay(TimeSpan.FromSeconds(1));

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

            currentTriggerEntity = await GetCurrentTriggerEntity();
            Assert.NotNull(currentTriggerEntity);
            Assert.Equal(2, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            tokenSource2.Cancel();
            await task2;
        }

        [Fact]
        public async Task GivenFailedTrigger_WhenReRunAsync_ThenTheTriggerShouldNotBeResumed()
        {
        }

        // TODO cancelled trigger

        private async Task<CurrentTriggerEntity> GetCurrentTriggerEntity() =>
            (await _metaDataTableClient.GetEntityAsync<CurrentTriggerEntity>(
                JobKeyProvider.TriggerPartitionKey((byte)QueueType.FhirToDataLake),
                JobKeyProvider.TriggerRowKey((byte)QueueType.FhirToDataLake),
                cancellationToken: CancellationToken.None)).Value;

        private async Task<TriggerLeaseEntity> GetTriggerLeaseEntity() =>
            (await _metaDataTableClient.GetEntityAsync<TriggerLeaseEntity>(
                JobKeyProvider.LeasePartitionKey((byte)QueueType.FhirToDataLake),
                JobKeyProvider.LeaseRowKey((byte)QueueType.FhirToDataLake),
                cancellationToken: CancellationToken.None)).Value;
    }
}
