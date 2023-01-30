// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using Microsoft.Health.JobManagement;
using NCrontab;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class DicomToDatalakeSchedulerServiceTests
    {
        private static readonly IDiagnosticLogger DiagnosticLogger = new DiagnosticLogger();

        private readonly NullLogger<DicomToDatalakeSchedulerService> _nullDicomSchedulerServiceLogger =
            NullLogger<DicomToDatalakeSchedulerService>.Instance;

        private static readonly MockMetricsLogger MetricsLogger = new MockMetricsLogger(new NullLogger<MockMetricsLogger>());

        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";
        private const string TestJobInfoQueueName = "jobinfoqueue";
        private const string TestJobInfoTableName = "jobinfotable";
        private const string TestMetadataTableName = "metadatatable";
        private const string TestWorkerName = "test-worker";
        private const string TestSchedulerCronExpression = "*/2 * * * * *";

        private readonly IOptions<JobConfiguration> _jobConfigOption;

        private const long TestEndOffset = 1300;

        public DicomToDatalakeSchedulerServiceTests()
        {
            var jobConfig = new JobConfiguration
            {
                QueueType = QueueType.DicomToDataLake,
                TableUrl = StorageEmulatorConnectionString,
                SchedulerCronExpression = TestSchedulerCronExpression,
                JobInfoTableName = TestJobInfoTableName,
                MetadataTableName = TestMetadataTableName,
                JobInfoQueueName = TestJobInfoQueueName,
            };

            _jobConfigOption = Options.Create(jobConfig);
            MetricsLogger.Clear();
        }

        // Initialize Test
        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new DicomToDatalakeSchedulerService(null, new MockMetadataStore(), GetMockDicomDataClient(string.Empty), Options.Create(new JobConfiguration()), MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger));

            Assert.Throws<ArgumentNullException>(
                () => new DicomToDatalakeSchedulerService(new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>(), null, GetMockDicomDataClient(string.Empty), Options.Create(new JobConfiguration()), MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger));

            Assert.Throws<ArgumentNullException>(
                () => new DicomToDatalakeSchedulerService(new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>(), new MockMetadataStore(), null, Options.Create(new JobConfiguration()), MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger));

            Assert.Throws<ArgumentNullException>(
                () => new DicomToDatalakeSchedulerService(new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>(), new MockMetadataStore(), GetMockDicomDataClient(string.Empty), null, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger));
        }

        [Theory]
        [InlineData("invalid cron expression")]
        [InlineData("100 */5 * * * *")]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("*/5 * * * *")]
        public void GivenInvalidSchedulerCronExpression_WhenInitialize_ExceptionShouldBeThrown(string schedulerCronExpression)
        {
            var jobConfig = new JobConfiguration
            {
                SchedulerCronExpression = schedulerCronExpression,
            };

            Assert.Throws<CrontabException>(
                () => new DicomToDatalakeSchedulerService(new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>(), new MockMetadataStore(), GetMockDicomDataClient(string.Empty), Options.Create(jobConfig), MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger));
        }

        [Fact]
        public void GivenNullSchedulerCronExpression_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new DicomToDatalakeSchedulerService(new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>(), new MockMetadataStore(), GetMockDicomDataClient(string.Empty), Options.Create(new JobConfiguration()), MetricsLogger,  DiagnosticLogger, _nullDicomSchedulerServiceLogger));
        }

        // Check Storage
        [Fact]
        public async Task GivenUninitializedStorage_WhenRunAsync_WaitUntilStorageInitialized()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();
            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                };

            // queue client isn't initialized
            queueClient.Initialized = false;

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

                CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
                Assert.Null(currentTriggerEntity);

                TriggerLeaseEntity triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
                Assert.Null(triggerLeaseEntity);

                // metadata store isn't initialized
                metadataStore.Initialized = false;
                queueClient.Initialized = true;

                await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

                currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
                Assert.Null(currentTriggerEntity);

                triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
                Assert.Null(triggerLeaseEntity);
                metadataStore.Initialized = true;

                await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

                currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
                Assert.NotNull(currentTriggerEntity);

                triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
                Assert.NotNull(triggerLeaseEntity);
            }
            finally
            {
                tokenSource.Cancel();
                await task;
            }
        }

        // Acquire Lease
        [Fact]
        public async Task GivenBrokenMetadataStoreThrowExceptionWhenGetTriggerLeaseEntity_WhenTryAcquireLeaseAsync_ThenNoExceptionShouldBeThrown()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var brokenMetadataStore = new MockMetadataStore
            {
                GetTriggerLeaseEntityFunc = (_, _, _) => throw new Exception("fake exception in get trigger lease entity"),
            };

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, brokenMetadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                };

            using var tokenSource = new CancellationTokenSource();

            tokenSource.CancelAfter(TimeSpan.FromSeconds(4));
            Exception exception = await Record.ExceptionAsync(async () => await schedulerService.RunAsync(tokenSource.Token));

            Assert.Null(exception);

            // No entity is added
            Assert.False(brokenMetadataStore.Entities.Any());

            // current trigger entity is null
            CurrentTriggerEntity currentTriggerEntity = await brokenMetadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Null(currentTriggerEntity);
        }

        [Fact]
        public async Task GivenBrokenMetadataStoreReturnNullWhenGetTriggerLeaseEntity_WhenTryAcquireLeaseAsync_ThenNoExceptionShouldBeThrown()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var brokenMetadataStore = new MockMetadataStore
            {
                GetTriggerLeaseEntityFunc = (_, _, _) => null,
            };

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, brokenMetadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                };

            using var tokenSource = new CancellationTokenSource();

            tokenSource.CancelAfter(TimeSpan.FromSeconds(2));
            Exception exception = await Record.ExceptionAsync(async () => await schedulerService.RunAsync(tokenSource.Token));

            Assert.Null(exception);

            // trigger lease entity is added
            Tuple<string, string> triggerLeaseEntityKey = new Tuple<string, string>(TableKeyProvider.LeasePartitionKey((byte)QueueType.DicomToDataLake), TableKeyProvider.LeaseRowKey((byte)QueueType.DicomToDataLake));

            Assert.True(brokenMetadataStore.Entities.ContainsKey(triggerLeaseEntityKey));

            // current trigger entity is null
            CurrentTriggerEntity currentTriggerEntity = await brokenMetadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Null(currentTriggerEntity);
        }

        [Fact]
        public async Task GivenNoTriggerLeaseEntityInTable_WhenTryAcquireLeaseAsync_ThenShouldCreateANewOne()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                };

            using var tokenSource = new CancellationTokenSource();

            tokenSource.CancelAfter(TimeSpan.FromSeconds(2));
            Exception exception = await Record.ExceptionAsync(async () => await schedulerService.RunAsync(tokenSource.Token));

            Assert.Null(exception);

            // trigger lease entity is added
            Tuple<string, string> triggerLeaseEntityKey = new Tuple<string, string>(TableKeyProvider.LeasePartitionKey((byte)QueueType.DicomToDataLake), TableKeyProvider.LeaseRowKey((byte)QueueType.DicomToDataLake));

            Assert.True(metadataStore.Entities.ContainsKey(triggerLeaseEntityKey));

            TriggerLeaseEntity triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(triggerLeaseEntity);

            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);
        }

        [Fact]
        public async Task GivenLeaseNotTimeout_WhenTryAcquireLeaseAsync_ThenShouldNotAcquireLease()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var oldGuid = Guid.NewGuid();
            var triggerLeaseEntity = new TriggerLeaseEntity
            {
                PartitionKey = TableKeyProvider.LeasePartitionKey((byte)QueueType.DicomToDataLake),
                RowKey = TableKeyProvider.LeaseRowKey((byte)QueueType.DicomToDataLake),
                WorkingInstanceGuid = oldGuid,
                HeartbeatDateTime = DateTime.UtcNow,
            };

            var metadataStore = new MockMetadataStore();

            // add the initial trigger entity to table
            await metadataStore.TryAddEntityAsync(triggerLeaseEntity, CancellationToken.None);

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                };

            using var tokenSource = new CancellationTokenSource();

            tokenSource.CancelAfter(TimeSpan.FromSeconds(4));
            Exception exception = await Record.ExceptionAsync(async () => await schedulerService.RunAsync(tokenSource.Token));

            Assert.Null(exception);

            // the scheduler service acquire lease
            TriggerLeaseEntity retrievedTriggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(oldGuid, retrievedTriggerLeaseEntity.WorkingInstanceGuid);
        }

        [Fact]
        public async Task GivenLeaseTimeout_WhenTryAcquireLeaseAsync_ThenShouldAcquireLease()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var oldGuid = Guid.NewGuid();
            var triggerLeaseEntity = new TriggerLeaseEntity
            {
                PartitionKey = TableKeyProvider.LeasePartitionKey((byte)QueueType.DicomToDataLake),
                RowKey = TableKeyProvider.LeaseRowKey((byte)QueueType.DicomToDataLake),
                WorkingInstanceGuid = oldGuid,
                HeartbeatDateTime = DateTime.UtcNow.AddMinutes(-1),
            };

            var metadataStore = new MockMetadataStore();

            // add the initial trigger entity to table
            await metadataStore.TryAddEntityAsync(triggerLeaseEntity, CancellationToken.None);

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();

            tokenSource.CancelAfter(TimeSpan.FromSeconds(4));
            Exception exception = await Record.ExceptionAsync(async () => await schedulerService.RunAsync(tokenSource.Token));

            Assert.Null(exception);

            // the scheduler service acquire lease
            TriggerLeaseEntity retrievedTriggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotEqual(oldGuid, retrievedTriggerLeaseEntity.WorkingInstanceGuid);
        }

        [Fact]
        public async Task GiveTwoSchedulerService_WhenTryAcquireLeaseAsync_ThenOnlyOneWillAcquireLease()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();

            // initialize two scheduler services
            var schedulerService1 =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            var schedulerService2 =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();

            Task task1 = schedulerService1.RunAsync(tokenSource.Token);
            Task task2 = schedulerService2.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);

            // one of the scheduler service acquire lease
            TriggerLeaseEntity triggerLeaseEntity1 = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(triggerLeaseEntity1);

            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);

            // the lease is renewed by the first scheduler service
            TriggerLeaseEntity triggerLeaseEntity2 = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(triggerLeaseEntity1.WorkingInstanceGuid, triggerLeaseEntity2.WorkingInstanceGuid);
            Assert.True(triggerLeaseEntity1.HeartbeatDateTime < triggerLeaseEntity2.HeartbeatDateTime);

            tokenSource.Cancel();
            await task1;
            await task2;

            // the scheduler service works correctly
            CurrentTriggerEntity currentTriggerEntity =
                await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);
        }

        [Fact]
        public async Task GivenCrashSchedulerService_WhenRunAsync_ThenTheLeaseShouldBeAcquiredByOtherSchedulerService()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();

            // initialize two scheduler services
            var schedulerService1 =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            var schedulerService2 =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            // service 1 is running
            using var tokenSource1 = new CancellationTokenSource();
            Task task1 = schedulerService1.RunAsync(tokenSource1.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);

            // service 1 acquire lease and run
            TriggerLeaseEntity triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(triggerLeaseEntity);
            Guid workingGuid1 = triggerLeaseEntity.WorkingInstanceGuid;

            // service 2 starts running
            using var tokenSource2 = new CancellationTokenSource();
            Task task2 = schedulerService2.RunAsync(tokenSource2.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);

            // scheduler service 2 can't acquire lease
            triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(workingGuid1, triggerLeaseEntity.WorkingInstanceGuid);

            // service 1 is stopped
            tokenSource1.Cancel();
            await task1;

            // scheduler service does not acquire lease as the lease isn't timeout
            triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(workingGuid1, triggerLeaseEntity.WorkingInstanceGuid);

            await Task.Delay(TimeSpan.FromSeconds(3), CancellationToken.None);

            // scheduler service 2 should acquire lease
            triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotEqual(workingGuid1, triggerLeaseEntity.WorkingInstanceGuid);

            tokenSource2.Cancel();
            await task2;
        }

        [Fact]
        public async Task GivenTwoSchedulerServiceAndLeaseTimeout_WhenTryAcquireLeaseAsync_ThenOnlyOneWillAcquireLease()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var oldGuid = Guid.NewGuid();
            var triggerLeaseEntity = new TriggerLeaseEntity
            {
                PartitionKey = TableKeyProvider.LeasePartitionKey((byte)QueueType.DicomToDataLake),
                RowKey = TableKeyProvider.LeaseRowKey((byte)QueueType.DicomToDataLake),
                WorkingInstanceGuid = oldGuid,
                HeartbeatDateTime = DateTime.UtcNow.AddMinutes(-1),
            };

            var metadataStore = new MockMetadataStore();

            // add the initial trigger entity to table
            await metadataStore.TryAddEntityAsync(triggerLeaseEntity, CancellationToken.None);

            var schedulerService1 =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            var schedulerService2 =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();

            tokenSource.CancelAfter(TimeSpan.FromSeconds(4));
            Task<Exception> task1 = Record.ExceptionAsync(async () => await schedulerService1.RunAsync(tokenSource.Token));
            Task<Exception> task2 = Record.ExceptionAsync(async () => await schedulerService2.RunAsync(tokenSource.Token));
            await Task.WhenAll(task1, task2);

            Assert.Null(task1.Result);
            Assert.Null(task2.Result);

            // one of the scheduler service acquire lease
            TriggerLeaseEntity triggerLeaseEntity1 = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotEqual(oldGuid, triggerLeaseEntity1.WorkingInstanceGuid);
        }

        // Renew Lease
        [Fact]
        public async Task GivenLongRunningSchedulerService_WhenRunAsync_ThenTheLeaseShouldBeRenewed()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);
            TriggerLeaseEntity triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(triggerLeaseEntity);

            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);

            TriggerLeaseEntity newTriggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(newTriggerLeaseEntity);

            // the lease is renewed.
            Assert.Equal(triggerLeaseEntity.WorkingInstanceGuid, newTriggerLeaseEntity.WorkingInstanceGuid);
            Assert.True(newTriggerLeaseEntity.HeartbeatDateTime > triggerLeaseEntity.HeartbeatDateTime);

            tokenSource.Cancel();
            await task;
        }

        [Fact]
        public async Task GivenLeaseLost_WhenRenewLease_ThenShouldFailToRenewLease()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(string.Empty), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 4,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);
            TriggerLeaseEntity triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(triggerLeaseEntity);
            Guid oldGuid = triggerLeaseEntity.WorkingInstanceGuid;
            DateTimeOffset oldHeartBeat = triggerLeaseEntity.HeartbeatDateTime;

            // the lease is acquired and updated by others
            var newGuid = Guid.NewGuid();
            triggerLeaseEntity.HeartbeatDateTime = DateTime.UtcNow;
            triggerLeaseEntity.WorkingInstanceGuid = newGuid;

            // add the initial trigger entity to table
            await metadataStore.TryUpdateEntityAsync(triggerLeaseEntity, CancellationToken.None);

            await Task.Delay(TimeSpan.FromSeconds(3), CancellationToken.None);
            TriggerLeaseEntity newTriggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.True(newTriggerLeaseEntity.HeartbeatDateTime > oldHeartBeat);
            Assert.Equal(triggerLeaseEntity.HeartbeatDateTime, newTriggerLeaseEntity.HeartbeatDateTime);
            Assert.NotEqual(oldGuid, newTriggerLeaseEntity.WorkingInstanceGuid);
            Assert.Equal(triggerLeaseEntity.WorkingInstanceGuid, newTriggerLeaseEntity.WorkingInstanceGuid);

            // the scheduler service acquire lease again
            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);

            newTriggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.True(newTriggerLeaseEntity.HeartbeatDateTime > triggerLeaseEntity.HeartbeatDateTime);
            Assert.NotEqual(triggerLeaseEntity.WorkingInstanceGuid, newTriggerLeaseEntity.WorkingInstanceGuid);
            Assert.Equal(oldGuid, newTriggerLeaseEntity.WorkingInstanceGuid);

            tokenSource.Cancel();
            await task;
        }

        // Enqueue Orchestrator Job
        [Fact]
        public async Task GivenEmptyDicomServer_WhenRunAsync_ThenShouldNotCreateTriggerEntity()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var metadataStore = new MockMetadataStore();

            var dataClient = Substitute.For<IApiDataClient>();
            dataClient.SearchAsync(default).ReturnsForAnyArgs(string.Empty);

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, dataClient, _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            // trigger lease entity is added
            TriggerLeaseEntity triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(triggerLeaseEntity);

            // current trigger entity is not added
            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Null(currentTriggerEntity);

            tokenSource.Cancel();
            await task;
        }

        [Fact]
        public async Task GivenNewJob_WhenRunAsync_ThenTheInitialTriggerEntityShouldBeCreatedAndEnqueueOrchestratorJob()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var metadataStore = new MockMetadataStore();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(TestDataProvider.GetDataFromFile(TestDataConstants.LatestChangeFeedFile2)), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            // should enqueue orchestrator job
            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);

            Assert.Equal(1, currentTriggerEntity.OrchestratorJobId);
            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
            Assert.Equal(0, currentTriggerEntity.StartOffset);
            Assert.Equal(TestEndOffset, currentTriggerEntity.EndOffset);

            tokenSource.Cancel();
            await task;
        }

        [Fact]
        public async Task GivenEnqueueFailure_WhenRunAsync_ThenTheTriggerShouldBeRetried()
        {
            var brokenQueueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            void FaultAction() => throw new RequestFailedException("fake request failed exception");

            brokenQueueClient.EnqueueFaultAction = FaultAction;

            var metadataStore = new MockMetadataStore();

            var schedulerService1 =
                new DicomToDatalakeSchedulerService(brokenQueueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            // service is running
            using var tokenSource1 = new CancellationTokenSource();
            Task task1 = schedulerService1.RunAsync(tokenSource1.Token);

            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);

            // the current trigger status is new.
            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);

            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.New, currentTriggerEntity.TriggerStatus);
            Assert.Equal(0, currentTriggerEntity.OrchestratorJobId);

            tokenSource1.Cancel();
            await task1;

            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var schedulerService2 =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
            {
                SchedulerServicePullingIntervalInSeconds = 1,
                SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                SchedulerServiceLeaseExpirationInSeconds = 2,
            };

            using var tokenSource2 = new CancellationTokenSource();
            Task task2 = schedulerService2.RunAsync(tokenSource2.Token);

            // wait for service 2 acquires lease
            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);

            // the current trigger status is running.
            currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);

            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
            Assert.Equal(1, currentTriggerEntity.OrchestratorJobId);

            tokenSource2.Cancel();
            await task2;
        }

        [Theory]
        [InlineData(JobVersion.V1)]
        public async Task GivenReEnqueueJob_WhenRunAsync_ThenTheExistingJobShouldBeReturned(JobVersion jobVersion)
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var initialTriggerEntity = new CurrentTriggerEntity
            {
                PartitionKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                RowKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                StartOffset = 0,
                EndOffset = 100,
                TriggerSequenceId = 0,
                JobVersion = jobVersion,
            };

            var metadataStore = new MockMetadataStore();

            // add the initial trigger entity to table
            await metadataStore.TryAddEntityAsync(initialTriggerEntity, CancellationToken.None);

            // enqueue manually
            var orchestratorDefinition1 = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = initialTriggerEntity.JobVersion,
                StartOffset = initialTriggerEntity.StartOffset,
                EndOffset = initialTriggerEntity.EndOffset,
            };

            List<JobInfo> jobInfoList = (await queueClient.EnqueueAsync(
                (byte)QueueType.DicomToDataLake,
                new[] { JsonConvert.SerializeObject(orchestratorDefinition1) },
                0,
                false,
                false,
                CancellationToken.None)).ToList();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
            {
                SchedulerServicePullingIntervalInSeconds = 0,
                SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                SchedulerServiceLeaseExpirationInSeconds = 2,
            };

            // service is running
            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);

            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
            Assert.Equal(jobInfoList.First().Id, currentTriggerEntity.OrchestratorJobId);

            JobInfo jobInfo = await queueClient.GetJobByIdAsync((byte)QueueType.DicomToDataLake, currentTriggerEntity.OrchestratorJobId, true, CancellationToken.None);

            Assert.Equal(jobInfoList.First().HeartbeatDateTime, jobInfo.HeartbeatDateTime);

            tokenSource.Cancel();
            await task;
        }

        // version update
        [Fact]
        public async Task GivenCompletedTriggerStatus_WhenUpdateVersion_ThenTheVersionOfNextJobIsUpdated()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            // trigger entity with old job version
            var triggerEntity = new CurrentTriggerEntity
            {
                PartitionKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                RowKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                TriggerStartTime = DateTime.UtcNow.AddMinutes(-10),
                TriggerEndTime = DateTime.UtcNow.AddMinutes(-5),
                TriggerStatus = TriggerStatus.Completed,
                TriggerSequenceId = 0,
                JobVersion = DicomToDatalakeJobVersionManager.DefaultJobVersion,
            };

            var metadataStore = new MockMetadataStore();

            // add the trigger entity to table
            await metadataStore.TryAddEntityAsync(triggerEntity, CancellationToken.None);

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            // service is running
            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);

            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);

            Assert.Equal(triggerEntity.TriggerSequenceId + 1, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            // The job version is updated
            Assert.Equal(DicomToDatalakeJobVersionManager.DefaultJobVersion, triggerEntity.JobVersion);
            Assert.Equal(DicomToDatalakeJobVersionManager.CurrentJobVersion, currentTriggerEntity.JobVersion);

            JobInfo jobInfo = await queueClient.GetJobByIdAsync((byte)QueueType.DicomToDataLake, currentTriggerEntity.OrchestratorJobId, true, CancellationToken.None);

            var inputData = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobInputData>(jobInfo.Definition);

            Assert.Equal(DicomToDatalakeJobVersionManager.CurrentJobVersion, inputData.JobVersion);

            tokenSource.Cancel();
            await task;
            Assert.Empty(MetricsLogger.MetricsDic);
        }

        [Fact]
        public async Task GivenNewTriggerStatus_WhenUpdateVersion_ThenTheVersionShouldNotBeUpdated()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            // trigger entity with old job version
            var triggerEntity = new CurrentTriggerEntity
            {
                PartitionKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                RowKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                TriggerStartTime = DateTime.UtcNow.AddMinutes(-10),
                TriggerEndTime = DateTime.UtcNow.AddMinutes(-5),
                TriggerStatus = TriggerStatus.New,
                TriggerSequenceId = 11,
                JobVersion = DicomToDatalakeJobVersionManager.DefaultJobVersion,
            };

            var metadataStore = new MockMetadataStore();

            // add the trigger entity to table
            await metadataStore.TryAddEntityAsync(triggerEntity, CancellationToken.None);

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            // service is running
            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);

            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);

            Assert.Equal(triggerEntity.TriggerSequenceId, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            // The job version is NOT updated
            Assert.Equal(DicomToDatalakeJobVersionManager.DefaultJobVersion, triggerEntity.JobVersion);
            Assert.Equal(DicomToDatalakeJobVersionManager.DefaultJobVersion, currentTriggerEntity.JobVersion);

            JobInfo jobInfo = await queueClient.GetJobByIdAsync((byte)QueueType.DicomToDataLake, currentTriggerEntity.OrchestratorJobId, true, CancellationToken.None);

            var inputData = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobInputData>(jobInfo.Definition);

            Assert.Equal(DicomToDatalakeJobVersionManager.DefaultJobVersion, inputData.JobVersion);

            tokenSource.Cancel();
            await task;
            Assert.Empty(MetricsLogger.MetricsDic);
        }

        [Fact]
        public async Task GivenJobEnqueuedTriggerNotUpdate_WhenUpdateVersion_ThenExistingJobShouldBeReturned()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            // trigger entity with old job version
            var triggerEntity = new CurrentTriggerEntity
            {
                PartitionKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                RowKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                TriggerStartTime = DateTime.UtcNow.AddMinutes(-10),
                TriggerEndTime = DateTime.UtcNow.AddMinutes(-5),
                TriggerStatus = TriggerStatus.New,
                JobVersion = DicomToDatalakeJobVersionManager.DefaultJobVersion,
            };

            var metadataStore = new MockMetadataStore();

            // add the trigger entity to table
            await metadataStore.TryAddEntityAsync(triggerEntity, CancellationToken.None);

            var orchestratorDefinition1 = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = triggerEntity.JobVersion,
                StartOffset = triggerEntity.StartOffset,
                EndOffset = triggerEntity.EndOffset,
            };

            List<JobInfo> jobInfoList = (await queueClient.EnqueueAsync(
                (byte)QueueType.DicomToDataLake,
                new[] { JsonConvert.SerializeObject(orchestratorDefinition1) },
                0,
                false,
                false,
                CancellationToken.None)).ToList();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 0,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            // service is running
            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);

            // return the existing job
            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(triggerEntity.TriggerSequenceId, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
            Assert.Equal(jobInfoList.First().Id, currentTriggerEntity.OrchestratorJobId);

            JobInfo jobInfo = await queueClient.GetJobByIdAsync((byte)QueueType.DicomToDataLake, currentTriggerEntity.OrchestratorJobId, true, CancellationToken.None);

            Assert.Equal(jobInfoList.First().HeartbeatDateTime, jobInfo.HeartbeatDateTime);

            // The job version is NOT updated
            Assert.Equal(DicomToDatalakeJobVersionManager.DefaultJobVersion, triggerEntity.JobVersion);

            var inputData = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobInputData>(jobInfo.Definition);

            Assert.Equal(DicomToDatalakeJobVersionManager.DefaultJobVersion, inputData.JobVersion);

            tokenSource.Cancel();
            await task;
            Assert.Empty(MetricsLogger.MetricsDic);
        }

        // Complete Trigger
        [Fact]
        public async Task GivenCompletedTrigger_WhenRunAsync_ThenShouldCreateAndEnqueueTheNextTrigger()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var triggerEntity = new CurrentTriggerEntity
            {
                PartitionKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                RowKey = TableKeyProvider.TriggerPartitionKey((byte)QueueType.DicomToDataLake),
                TriggerEndTime = DateTime.UtcNow.AddMinutes(-5),
                TriggerStatus = TriggerStatus.Completed,
                TriggerSequenceId = 11,
                StartOffset = 0,
                EndOffset = 100,
            };

            var metadataStore = new MockMetadataStore();

            // add the trigger entity to table
            await metadataStore.TryAddEntityAsync(triggerEntity, CancellationToken.None);

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(TestDataProvider.GetDataFromFile(TestDataConstants.LatestChangeFeedFile2)), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(triggerEntity.TriggerSequenceId + 1, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            Assert.Equal(triggerEntity.EndOffset, currentTriggerEntity.StartOffset);
            Assert.Equal(TestEndOffset, currentTriggerEntity.EndOffset);

            JobInfo jobInfo = await queueClient.GetJobByIdAsync((byte)QueueType.DicomToDataLake, currentTriggerEntity.OrchestratorJobId, true, CancellationToken.None);

            Assert.NotNull(jobInfo);

            tokenSource.Cancel();
            await task;
        }

        [Fact]
        public async Task GivenNoNewData_WhenCreateNextTrigger_ThenShouldNotCreateNextTrigger()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var metadataStore = new MockMetadataStore();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(TestDataProvider.GetDataFromFile(TestDataConstants.LatestChangeFeedFile2)), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            // should enqueue orchestrator job
            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);

            Assert.Equal(1, currentTriggerEntity.OrchestratorJobId);
            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
            Assert.Equal(0, currentTriggerEntity.StartOffset);
            Assert.Equal(TestEndOffset, currentTriggerEntity.EndOffset);

            DateTimeOffset lastTriggerEndTime = currentTriggerEntity.TriggerEndTime;

            // the job is dequeued, and the trigger status is still running.
            JobInfo jobInfo = await queueClient.DequeueAsync(
                (byte)QueueType.DicomToDataLake,
                TestWorkerName,
                0,
                CancellationToken.None);

            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);

            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            // job is completed
            jobInfo.Status = JobStatus.Completed;
            await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

            // nenxt trigger entity is not created
            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);
            currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);
            Assert.Equal(1, currentTriggerEntity.OrchestratorJobId);
            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Completed, currentTriggerEntity.TriggerStatus);
            Assert.Equal(TestEndOffset, currentTriggerEntity.EndOffset);

            tokenSource.Cancel();
            await task;
        }

        // Rerun test
        [Fact]
        public async Task GivenRunningTrigger_WhenReRunAsync_ThenTheTriggerShouldBePickedUp()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            // service is running
            using var tokenSource1 = new CancellationTokenSource();
            Task task1 = schedulerService.RunAsync(tokenSource1.Token);

            // should enqueue orchestrator job
            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);

            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            // the job is dequeued
            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);
            JobInfo jobInfo = await queueClient.DequeueAsync(
                (byte)QueueType.DicomToDataLake,
                TestWorkerName,
                0,
                CancellationToken.None);

            // job is completed, the trigger status should be set to next trigger
            jobInfo.Status = JobStatus.Completed;
            await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);

            tokenSource1.Cancel();
            await task1;

            // task1 stops, the trigger status is running, trigger sequence id is 1.
            currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);
            Assert.Equal(1, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            // re-run task2
            using var tokenSource2 = new CancellationTokenSource();
            Task task2 = schedulerService.RunAsync(tokenSource2.Token);

            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);

            // resume trigger
            currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);
            Assert.Equal(1, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            jobInfo = await queueClient.DequeueAsync(
                (byte)QueueType.DicomToDataLake,
                TestWorkerName,
                0,
                CancellationToken.None);

            // job is completed, the trigger status should be set to completed
            jobInfo.Status = JobStatus.Completed;
            await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);

            currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);
            Assert.Equal(TriggerStatus.Completed, currentTriggerEntity.TriggerStatus);
            Assert.Equal(TestEndOffset, currentTriggerEntity.EndOffset);

            tokenSource2.Cancel();
            await task2;
        }

        [Fact]
        public async Task GivenFailedTrigger_WhenReRunAsync_ThenTheTriggerShouldKeepFailure()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var metadataStore = new MockMetadataStore();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            // service is running
            using var tokenSource1 = new CancellationTokenSource();
            Task task1 = schedulerService.RunAsync(tokenSource1.Token);

            // the job is dequeued
            JobInfo jobInfo = null;
            while (jobInfo == null)
            {
                await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

                jobInfo = await queueClient.DequeueAsync(
                    (byte)QueueType.DicomToDataLake,
                    TestWorkerName,
                    0,
                    CancellationToken.None);
            }

            // job is failed
            jobInfo.Status = JobStatus.Failed;
            await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

            // cancel the scheduler service
            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);
            tokenSource1.Cancel();
            await task1;

            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);

            // current trigger entity is failed
            Assert.Equal(jobInfo.Id, currentTriggerEntity.OrchestratorJobId);
            Assert.Equal(TriggerStatus.Failed, currentTriggerEntity.TriggerStatus);
            TriggerLeaseEntity triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);

            using var tokenSource2 = new CancellationTokenSource();
            Task task2 = schedulerService.RunAsync(tokenSource2.Token);
            tokenSource2.CancelAfter(TimeSpan.FromSeconds(4));

            await task2;

            // the currentTriggerEntity should not be updated
            CurrentTriggerEntity currentTriggerEntity2 = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity2);

            // current trigger entity is failed
            Assert.Equal(jobInfo.Id, currentTriggerEntity2.OrchestratorJobId);
            Assert.Equal(TriggerStatus.Failed, currentTriggerEntity2.TriggerStatus);
            Assert.Equal(currentTriggerEntity.Timestamp, currentTriggerEntity2.Timestamp);

            // lease should be renewed
            TriggerLeaseEntity triggerLeaseEntity2 = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(triggerLeaseEntity.WorkingInstanceGuid, triggerLeaseEntity2.WorkingInstanceGuid);
            Assert.True(triggerLeaseEntity2.HeartbeatDateTime > triggerLeaseEntity.HeartbeatDateTime);
        }

        [Fact]
        public async Task GivenCancelledTrigger_WhenReRunAsync_ThenTheTriggerShouldKeepFailure()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var metadataStore = new MockMetadataStore();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            // service is running
            using var tokenSource1 = new CancellationTokenSource();
            Task task1 = schedulerService.RunAsync(tokenSource1.Token);

            // the job is dequeued
            JobInfo jobInfo = null;
            while (jobInfo == null)
            {
                await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

                jobInfo = await queueClient.DequeueAsync(
                    (byte)QueueType.DicomToDataLake,
                    TestWorkerName,
                    0,
                    CancellationToken.None);
            }

            // job is cancelled
            jobInfo.Status = JobStatus.Cancelled;
            await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

            // cancel the scheduler service
            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);
            tokenSource1.Cancel();
            await task1;

            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);

            // current trigger entity is cancelled
            Assert.Equal(jobInfo.Id, currentTriggerEntity.OrchestratorJobId);
            Assert.Equal(TriggerStatus.Cancelled, currentTriggerEntity.TriggerStatus);
            TriggerLeaseEntity triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);

            using var tokenSource2 = new CancellationTokenSource();
            Task task2 = schedulerService.RunAsync(tokenSource2.Token);
            tokenSource2.CancelAfter(TimeSpan.FromSeconds(4));

            await task2;

            // the currentTriggerEntity should not be updated
            CurrentTriggerEntity currentTriggerEntity2 = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity2);

            // current trigger entity is cancelled
            Assert.Equal(jobInfo.Id, currentTriggerEntity2.OrchestratorJobId);
            Assert.Equal(TriggerStatus.Cancelled, currentTriggerEntity2.TriggerStatus);
            Assert.Equal(currentTriggerEntity.Timestamp, currentTriggerEntity2.Timestamp);

            // lease should be renewed
            TriggerLeaseEntity triggerLeaseEntity2 = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(triggerLeaseEntity.WorkingInstanceGuid, triggerLeaseEntity2.WorkingInstanceGuid);
            Assert.True(triggerLeaseEntity2.HeartbeatDateTime > triggerLeaseEntity.HeartbeatDateTime);
        }

        // Cancellation Request
        [Fact]
        public async Task GivenCancelRequest_WhenStartToRun_ThenSchedulerShouldBeCancelledWithoutDelay()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();
            var schedulerService =
                    new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                    {
                        SchedulerServicePullingIntervalInSeconds = 1,
                        SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                        SchedulerServiceLeaseExpirationInSeconds = 2,
                    };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            // cancel after 10ms
            tokenSource.CancelAfter(TimeSpan.FromMilliseconds(10));

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            await task;
            stopWatch.Stop();
            long elapsedSeconds = stopWatch.ElapsedMilliseconds / 1000;
            Assert.True(elapsedSeconds < schedulerService.SchedulerServicePullingIntervalInSeconds);
            Assert.True(elapsedSeconds < schedulerService.SchedulerServiceLeaseRefreshIntervalInSeconds);
        }

        [Fact]
        public async Task GivenCancelRequest_WhenCheckStorageInitialization_ThenSchedulerShouldBeCancelledWithoutDelay()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();
            queueClient.Initialized = false;
            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            // cancel after 2 seconds
            await Task.Delay(TimeSpan.FromSeconds(3), CancellationToken.None);
            tokenSource.Cancel();

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            await task;
            stopWatch.Stop();
            long elapsedSeconds = stopWatch.ElapsedMilliseconds / 1000;
            Assert.True(elapsedSeconds < schedulerService.SchedulerServicePullingIntervalInSeconds);
            Assert.True(elapsedSeconds < schedulerService.SchedulerServiceLeaseRefreshIntervalInSeconds);
        }

        [Fact]
        public async Task GivenCancelRequest_WhenTryAcquireLease_ThenSchedulerShouldBeCancelledWithoutDelay()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            // the lease is acquired by another instance
            var otherGuid = Guid.NewGuid();
            var triggerLeaseEntity = new TriggerLeaseEntity
            {
                PartitionKey = TableKeyProvider.LeasePartitionKey((byte)QueueType.DicomToDataLake),
                RowKey = TableKeyProvider.LeaseRowKey((byte)QueueType.DicomToDataLake),
                WorkingInstanceGuid = otherGuid,
                HeartbeatDateTime = DateTime.UtcNow,
            };

            var metadataStore = new MockMetadataStore();

            // add the initial trigger entity to table
            await metadataStore.TryAddEntityAsync(triggerLeaseEntity, CancellationToken.None);

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            // cancel after 4 seconds
            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);
            tokenSource.Cancel();

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            await task;
            stopWatch.Stop();
            long elapsedSeconds = stopWatch.ElapsedMilliseconds / 1000;
            Assert.True(elapsedSeconds < schedulerService.SchedulerServicePullingIntervalInSeconds);
            Assert.True(elapsedSeconds < schedulerService.SchedulerServiceLeaseRefreshIntervalInSeconds);

            // the service fails to acquire lease
            TriggerLeaseEntity retrievedTriggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.Equal(otherGuid, retrievedTriggerLeaseEntity.WorkingInstanceGuid);
        }

        [Fact]
        public async Task GivenCancelRequest_WhenRunAsync_ThenSchedulerShouldBeCancelledWithoutDelay()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();
            var metadataStore = new MockMetadataStore();
            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None);
            TriggerLeaseEntity triggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(triggerLeaseEntity);

            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            TriggerLeaseEntity newTriggerLeaseEntity = await metadataStore.GetTriggerLeaseEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(newTriggerLeaseEntity);

            // the lease is renewed.
            Assert.Equal(triggerLeaseEntity.WorkingInstanceGuid, newTriggerLeaseEntity.WorkingInstanceGuid);
            Assert.True(newTriggerLeaseEntity.HeartbeatDateTime > triggerLeaseEntity.HeartbeatDateTime);

            // The job is running
            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);

            Assert.Equal(1, currentTriggerEntity.OrchestratorJobId);
            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
            Assert.Null(currentTriggerEntity.TriggerStartTime);

            tokenSource.Cancel();

            var stopWatch = new Stopwatch();
            stopWatch.Start();
            await task;
            stopWatch.Stop();
            long elapsedSeconds = stopWatch.ElapsedMilliseconds / 1000;
            Assert.True(elapsedSeconds < schedulerService.SchedulerServicePullingIntervalInSeconds);
            Assert.True(elapsedSeconds < schedulerService.SchedulerServiceLeaseRefreshIntervalInSeconds);
        }

        // Functional Test
        [Fact]
        public async Task GivenValidTrigger_WhenRunAsync_ThenTheTriggerShouldBeProcessed()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var metadataStore = new MockMetadataStore();

            var schedulerService =
                new DicomToDatalakeSchedulerService(queueClient, metadataStore, GetMockDicomDataClient(), _jobConfigOption, MetricsLogger, DiagnosticLogger, _nullDicomSchedulerServiceLogger)
                {
                    SchedulerServicePullingIntervalInSeconds = 1,
                    SchedulerServiceLeaseRefreshIntervalInSeconds = 1,
                    SchedulerServiceLeaseExpirationInSeconds = 2,
                };

            using var tokenSource = new CancellationTokenSource();
            Task task = schedulerService.RunAsync(tokenSource.Token);

            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            // should enqueue orchestrator job
            CurrentTriggerEntity currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);

            Assert.Equal(1, currentTriggerEntity.OrchestratorJobId);
            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
            Assert.Equal(0, currentTriggerEntity.StartOffset);
            Assert.Equal(1265, currentTriggerEntity.EndOffset);

            DateTimeOffset lastTriggerEndTime = currentTriggerEntity.TriggerEndTime;

            // the job is dequeued, and the trigger status is still running.
            JobInfo jobInfo = await queueClient.DequeueAsync(
                (byte)QueueType.DicomToDataLake,
                TestWorkerName,
                0,
                CancellationToken.None);

            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);

            Assert.Equal(0, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);

            // job is completed, the trigger status should be set to next trigger
            jobInfo.Status = JobStatus.Completed;
            await queueClient.CompleteJobAsync(jobInfo, false, CancellationToken.None);

            // current trigger entity is running
            await Task.Delay(TimeSpan.FromSeconds(4), CancellationToken.None);
            currentTriggerEntity = await metadataStore.GetCurrentTriggerEntityAsync((byte)QueueType.DicomToDataLake, CancellationToken.None);
            Assert.NotNull(currentTriggerEntity);
            Assert.Equal(2, currentTriggerEntity.OrchestratorJobId);
            Assert.Equal(1, currentTriggerEntity.TriggerSequenceId);
            Assert.Equal(TriggerStatus.Running, currentTriggerEntity.TriggerStatus);
            Assert.Equal(1265, currentTriggerEntity.StartOffset);
            Assert.Equal(TestEndOffset, currentTriggerEntity.EndOffset);

            // the next job is created
            jobInfo =
                await queueClient.GetJobByIdAsync((byte)QueueType.DicomToDataLake, 2, true, CancellationToken.None);
            Assert.Equal(JobStatus.Created, jobInfo.Status);

            tokenSource.Cancel();
            await task;
        }

        private static IApiDataClient GetMockDicomDataClient(string firstBundle = null)
        {
            if (firstBundle == null)
            {
                firstBundle = TestDataProvider.GetDataFromFile(TestDataConstants.LatestChangeFeedFile1);
            }

            var dataClient = Substitute.For<IApiDataClient>();

            // Get bundle from next link
            string nextBundle = TestDataProvider.GetDataFromFile(TestDataConstants.LatestChangeFeedFile2);
            dataClient.SearchAsync(default).ReturnsForAnyArgs(firstBundle, nextBundle);
            return dataClient;
        }
    }
}