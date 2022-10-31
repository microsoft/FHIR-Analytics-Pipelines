// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure;
using Azure.Data.Tables;
using Azure.Storage.Queues;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.JobManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage;
using Microsoft.Health.JobManagement;
using NSubstitute;
using Xunit;
using JobStatus = Microsoft.Health.JobManagement.JobStatus;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.UnitTests
{
#pragma warning disable SA1602 // Enumeration items should be documented
    /// <summary>
    /// Use different queue type for integration test to avoid conflict
    /// </summary>
    internal enum TestQueueType : byte
    {
        GivenJobsLargerThanTransactionLimitation_WhenEnqueue_ThenTheExceptionShouldBeThrown = 16,
        GivenLargeSizeJob_WhenEnqueueJobs_ThenTheExceptionShouldBeThrown,
        GivenNewJobs_WhenEnqueueJobs_ThenCreatedJobsShouldBeReturned,
        GivenNewJobsWithSameQueueType_WhenEnqueueWithForceOneActiveJobGroup_ThenSecondJobShouldNotBeEnqueued,
        GivenJobsWithSameDefinition_WhenEnqueueConcurrently_ThenOnlyOneJobShouldBeEnqueued,
        GivenJobsWithSameDefinition_WhenEnqueueInABatch_ThenTheExceptionShouldBeThrown,
        GivenJobsWithSameDefinition_WhenEnqueue_ThenOnlyOneJobShouldBeEnqueued,
        GivenCreatedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned,
        GivenRunningJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned,
        GivenFinishedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned,
        GivenEnqueueFailed_WhenEnqueueJobAgain_ThenContinueToEnqueue,

        GivenJobsEnqueue_WhenDequeue_ThenAllJobsShouldBeReturned,
        GivenJobNotHeartbeat_WhenDequeue_ThenJobShouldBeReturnedAgain,
        GivenJobKeepPutHeartbeat_WhenDequeue_ThenJobShouldNotBeReturned,
        GivenJobKeepPutHeartbeatWithResult_WhenDequeue_ThenJobWithResultShouldBeReturned,
        GivenEmptyQueue_WhenDequeue_ThenNoResultShouldBeReturned,
        GivenJobsEnqueue_WhenDequeueConcurrently_ThenCorrectResultShouldBeReturned,
        GivenFinishedJobs_WhenDequeue_ThenNoResultShouldBeReturned,
        GivenJobCancelRequested_WhenDequeue_ThenTheJobShouldBeReturned,
        GivenNullMessageInTable_WhenDequeue_ThenTheMessageWillBeSkipped,
        GivenInvalidMessage_WhenDequeue_ThenTheExceptionShouldBeThrown,
        GivenMessageWithoutTableEntity_WhenDequeue_ThenTheExceptionShouldBeThrown,
        GivenMessageInconsistentWithTableEntity_WhenDequeue_ThenTheExceptionShouldBeThrown,

        GivenJobId_WhenGetJobById_ThenTheJobShouldBeReturned,
        GivenJobsEnqueue_WhenGetJobWithReturnDefinitionFalse_ThenTheDefinitionShouldNotBeReturned,
        GivenJobId_WhenGetJobByIdConcurrently_ThenTheJobShouldBeReturned,
        GivenNotExistJobId_WhenGetJobById_ThenTheExceptionShouldBeThrown,

        GivenJobsEnqueue_WhenGetJobsByIds_ThenTheJobsShouldBeReturned,
        GivenNotExistJobId_WhenGetJobsByIds_ThenTheExceptionShouldBeThrown,

        GivenGroupJobs_WhenGetJobByGroupId_ThenTheCorrectJobsShouldBeReturned,
        GivenNoJobInGroup_WhenGetJobByGroupId_ThenNoJobShouldBeReturned,
        GivenGroupJobs_WhenGetJobByGroupIdWithReturnDefinitionFalse_ThenTheDefinitionShouldNotBeReturned,

        GivenGroupJobs_WhenCancelJobByGroupId_ThenAllJobsShouldBeCancelled,
        GivenJobsWithDifferentStatus_WhenCancelJobByGroupId_ThenTheStatusShouldBeSetCorrectly,
        GivenNoJobInGroup_WhenCancelJobByGroupId_ThenNoExceptionShouldBeThrown,

        GivenGroupJobs_WhenCancelJobById_ThenOnlySingleJobShouldBeCancelled,
        GivenJobsWithDifferentStatus_WhenCancelJobById_ThenTheStatusShouldBeSetCorrectly,
        GivenNoExistJob_WhenCancelJobById_ThenTheExceptionShouldBeThrown,

        GivenJobWithResult_WhenKeepAlive_ThenTheResultShouldBeUpdated,
        GivenRunningJobCancelled_WhenKeepAlive_ThenCancelRequestedShouldBeReturned,
        GivenRunningJob_WhenKeepAliveFailed_ThenTheJobShouldBeDequeuedAgain,

        GivenGroupJobs_WhenCompleteJob_ThenJobsShouldBeCompleted,
        GivenGroupJobs_WhenOneJobFailedAndRequestCancellation_ThenAllJobsShouldBeCancelled,
        GivenCancelledJobs_WhenCompleteJob_ThenTheJobStatusShouldBeCorrect,
    }
#pragma warning restore SA1602 // Enumeration items should be documented

    [Trait("Category", "JobManagementTests")]
    public class AzureStorageJobQueueClientTests
    {
        private readonly NullLogger<AzureStorageJobQueueClient<AzureStorageJobInfo>>
            _nullAzureStorageJobQueueClientLogger =
                NullLogger<AzureStorageJobQueueClient<AzureStorageJobInfo>>.Instance;

        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";
        private const string TestJobInfoTableName = "testjobinfotable";
        private const string TestJobInfoQueueName = "testjobinfoqueue";
        private const string TestWorkerName = "test-worker";
        private const int HeartbeatTimeoutSec = 2;

        private readonly AzureStorageJobQueueClient<AzureStorageJobInfo> _azureStorageJobQueueClient;
        private readonly TableClient _azureJobInfoTableClient;
        private readonly QueueClient _azureJobMessageQueueClient;

        public AzureStorageJobQueueClientTests()
        {
            _azureJobInfoTableClient =
                new TableClient(StorageEmulatorConnectionString, TestJobInfoTableName);
            _azureJobMessageQueueClient =
                new QueueClient(StorageEmulatorConnectionString, TestJobInfoQueueName);

            // Delete table and queue if exists
            _azureJobInfoTableClient.Delete();
            _azureJobMessageQueueClient.DeleteIfExists();

            var azureStorageClientFactory = new AzureStorageClientFactory(
                TestJobInfoTableName,
                TestJobInfoQueueName,
                new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()),
                new NullLogger<AzureStorageClientFactory>());

            _azureStorageJobQueueClient = new AzureStorageJobQueueClient<AzureStorageJobInfo>(
                azureStorageClientFactory,
                _nullAzureStorageJobQueueClientLogger);

            _azureStorageJobQueueClient.IsInitialized();
        }

        // IsInitialized
        [Fact]
        public async Task GivenNoExistStorage_WhenCheckIsInitialized_ThenTheStorageShouldBeCreated()
        {
            const string tableName = "newtablename";
            const string queueName = "newqueuename";
            var tableClient = new TableClient(StorageEmulatorConnectionString, tableName);
            var queueClient = new QueueClient(StorageEmulatorConnectionString, queueName);

            try
            {
                // the table should not exist
                Assert.Equal(404, (await tableClient.DeleteAsync()).Status);

                // the queue should not exist
                Assert.False(await queueClient.DeleteIfExistsAsync());

                var azureStorageClientFactory = new AzureStorageClientFactory(
                    tableName,
                    queueName,
                    new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()),
                    new NullLogger<AzureStorageClientFactory>());

                // create a AzureStorageJobQueueClient
                AzureStorageJobQueueClient<AzureStorageJobInfo> azureStorageJobQueueClient =
                    new AzureStorageJobQueueClient<AzureStorageJobInfo>(
                        azureStorageClientFactory,
                        _nullAzureStorageJobQueueClientLogger);

                // the storage should still not exist
                Assert.Equal(404, (await tableClient.DeleteAsync()).Status);
                Assert.False(await queueClient.DeleteIfExistsAsync());

                // will create the storage and return true
                Assert.True(azureStorageJobQueueClient.IsInitialized());

                // the storage is created
                Assert.Equal(204, (await tableClient.DeleteAsync()).Status);
                Assert.True(await queueClient.DeleteIfExistsAsync());
            }
            finally
            {
                await tableClient.DeleteAsync();
                await queueClient.DeleteIfExistsAsync();
            }
        }

        [Fact]
        public async Task GivenExistStorage_WhenCheckIsInitialized_ThenShouldReturnTrue()
        {
            const string tableName = "newtablename";
            const string queueName = "newqueuename";
            var tableClient = new TableClient(StorageEmulatorConnectionString, tableName);
            var queueClient = new QueueClient(StorageEmulatorConnectionString, queueName);

            try
            {
                await tableClient.CreateIfNotExistsAsync();
                await queueClient.CreateIfNotExistsAsync();

                var azureStorageClientFactory = new AzureStorageClientFactory(
                    tableName,
                    queueName,
                    new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()),
                    new NullLogger<AzureStorageClientFactory>());

                // create a AzureStorageJobQueueClient
                AzureStorageJobQueueClient<AzureStorageJobInfo> azureStorageJobQueueClient =
                    new AzureStorageJobQueueClient<AzureStorageJobInfo>(
                        azureStorageClientFactory,
                        _nullAzureStorageJobQueueClientLogger);

                // should return true
                Assert.True(azureStorageJobQueueClient.IsInitialized());

                // the storage is created
                Assert.Equal(204, (await tableClient.DeleteAsync()).Status);
                Assert.True(await queueClient.DeleteIfExistsAsync());
            }
            catch (Exception)
            {
                // should not throw any exception
                Assert.True(false);
            }
            finally
            {
                await tableClient.DeleteAsync();
                await queueClient.DeleteIfExistsAsync();
            }
        }

        [Fact]
        public void GivenBrokenStorage_WhenCreateStorage_ThenCheckIsInitializedShouldReturnFalse()
        {
            var azureStorageClientFactory = Substitute.For<IAzureStorageClientFactory>();
            azureStorageClientFactory.CreateTableClient().Returns(_ =>
            {
                var brokenTableClient = Substitute.For<TableClient>();
                brokenTableClient.CreateIfNotExists().Returns(_ => throw new Exception("fake exception"));
                return brokenTableClient;
            });

            // create a AzureStorageJobQueueClient
            AzureStorageJobQueueClient<AzureStorageJobInfo> azureStorageJobQueueClient =
                new AzureStorageJobQueueClient<AzureStorageJobInfo>(
                    azureStorageClientFactory,
                    _nullAzureStorageJobQueueClientLogger);

            // should return false
            Assert.False(azureStorageJobQueueClient.IsInitialized());
        }

        // Enqueue
        [Fact]
        public async Task GivenJobsLargerThanTransactionLimitation_WhenEnqueue_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsLargerThanTransactionLimitation_WhenEnqueue_ThenTheExceptionShouldBeThrown;

            const int jobCnt = 51;
            List<string> definitions = new List<string>();
            for (int i = 0; i < jobCnt; i++)
            {
                definitions.Add($"job{i}");
            }

            await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.EnqueueAsync(
                    queueType,
                    definitions.ToArray(),
                    null,
                    false,
                    false,
                    CancellationToken.None));
        }

        [Fact]
        public async Task GivenUpdateJobIdEntityConflict_WhenEnqueueJobs_ThenTheExceptionShouldBeThrownAfterRetry()
        {
            var azureStorageClientFactory = Substitute.For<IAzureStorageClientFactory>();
            var brokenTableClient = Substitute.For<TableClient>();
            string partitionKey = AzureStorageKeyProvider.JobIdPartitionKey(0);
            string rowKey = AzureStorageKeyProvider.JobIdRowKey(0);
            var initialJobIdEntity = new JobIdEntity
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,
                NextJobId = 0,
            };

            brokenTableClient.GetEntityAsync<JobIdEntity>(partitionKey, rowKey)
                .Returns(Task.FromResult(Response.FromValue(initialJobIdEntity, Substitute.For<Response>())));

            brokenTableClient.AddEntityAsync(new JobIdEntity()).ReturnsForAnyArgs(Substitute.For<Response>());

            brokenTableClient.UpdateEntityAsync(new JobIdEntity(), default).ReturnsForAnyArgs<Task<Response>>(_ => throw new RequestFailedException(0, "fake exception", "UpdateConditionNotSatisfied", null));
            azureStorageClientFactory.CreateTableClient().Returns(_ => brokenTableClient);

            // create a AzureStorageJobQueueClient
            AzureStorageJobQueueClient<AzureStorageJobInfo> azureStorageJobQueueClient =
                new AzureStorageJobQueueClient<AzureStorageJobInfo>(
                    azureStorageClientFactory,
                    _nullAzureStorageJobQueueClientLogger);

            string[] definitions = new[] { "job1" };

            await Assert.ThrowsAsync<JobManagementException>(async () =>
                await azureStorageJobQueueClient.EnqueueAsync(0, definitions, null, false, false, CancellationToken.None));

            // throw exception after retry 5 times
            await brokenTableClient.Received(6).UpdateEntityAsync(Arg.Any<JobIdEntity>(), default);
        }

        [Fact]
        public async Task GivenLargeSizeJob_WhenEnqueueJobs_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenLargeSizeJob_WhenEnqueueJobs_ThenTheExceptionShouldBeThrown;

            string[] definitions = new[] { new string('a', 1024 * 1024 * 8) };

            Exception exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.EnqueueAsync(
                    queueType,
                    definitions.ToArray(),
                    null,
                    false,
                    false,
                    CancellationToken.None));
            Assert.Equal("The table entity exceeds the the maximum allowed size (1MB).", exception.Message);

            definitions = new[] { new string('a', 64 * 1024) };

            exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.EnqueueAsync(
                    queueType,
                    definitions.ToArray(),
                    null,
                    false,
                    false,
                    CancellationToken.None));
            Assert.Equal("The property value exceeds the maximum allowed size (64KB).", exception.Message);
        }

        [Fact]
        public async Task GivenNewJobs_WhenEnqueueJobs_ThenCreatedJobsShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenNewJobs_WhenEnqueueJobs_ThenCreatedJobsShouldBeReturned;

            string[] definitions = new[] { "job1", "job2" };
            List<JobInfo> jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                definitions,
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            // check job info
            Assert.Equal(2, jobInfos.Count);
            Assert.Equal(1, jobInfos.Last().Id - jobInfos.First().Id);
            Assert.Equal(JobStatus.Created, jobInfos.First().Status);
            Assert.Null(jobInfos.First().StartDate);
            Assert.Null(jobInfos.First().EndDate);
            Assert.Equal(jobInfos.Last().GroupId, jobInfos.First().GroupId);

            await CheckJob(jobInfos.First());
            await CheckJob(jobInfos.Last());

            JobInfo jobInfo =
                await _azureStorageJobQueueClient.GetJobByIdAsync(
                    queueType,
                    jobInfos.First().Id,
                    true,
                    CancellationToken.None);
            Assert.Contains(jobInfo.Definition, definitions);

            jobInfo = await _azureStorageJobQueueClient.GetJobByIdAsync(
                queueType,
                jobInfos.Last().Id,
                true,
                CancellationToken.None);
            Assert.Contains(jobInfo.Definition, definitions);
        }

        [Fact]
        public async Task
            GivenNewJobsWithSameQueueType_WhenEnqueueWithForceOneActiveJobGroup_ThenSecondJobShouldNotBeEnqueued()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenNewJobsWithSameQueueType_WhenEnqueueWithForceOneActiveJobGroup_ThenSecondJobShouldNotBeEnqueued;

            await _azureStorageJobQueueClient.EnqueueAsync(queueType, new[] { "job1" }, null, true, false, CancellationToken.None);

            // TODO: this field ForceOneActiveJobGroup isn't implemented
            // await Assert.ThrowsAsync<JobConflictException>(async () => await _azureStorageJobQueueClient.EnqueueAsync(queueType, new string[] { "job2" }, null, true, false, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobsWithSameDefinition_WhenEnqueueConcurrently_ThenOnlyOneJobShouldBeEnqueued()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenJobsWithSameDefinition_WhenEnqueueConcurrently_ThenOnlyOneJobShouldBeEnqueued;

            List<Task<IEnumerable<JobInfo>>>? tasks = new List<Task<IEnumerable<JobInfo>>>();
            Task<IEnumerable<JobInfo>>? task1 = _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            Task<IEnumerable<JobInfo>>? task2 = _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            tasks.Add(task1);
            tasks.Add(task2);

            IEnumerable<JobInfo>[] result = await Task.WhenAll(tasks);
            Assert.Single(result[0]);
            Assert.Single(result[1]);
            Assert.Equal(result[0].ToList().First().Id, result[1].ToList().First().Id);
        }

        [Fact]
        public async Task GivenJobsWithSameDefinition_WhenEnqueueInABatch_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsWithSameDefinition_WhenEnqueueInABatch_ThenTheExceptionShouldBeThrown;

            await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.EnqueueAsync(
                    queueType,
                    new[] { "job1", "job1" },
                    null,
                    false,
                    false,
                    CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobsWithSameDefinition_WhenEnqueue_ThenOnlyOneJobShouldBeEnqueued()
        {
            await CleanStorage();

            const byte queueType =
                (byte)TestQueueType.GivenJobsWithSameDefinition_WhenEnqueue_ThenOnlyOneJobShouldBeEnqueued;

            List<JobInfo> jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfos);
            long jobId = jobInfos.First().Id;
            jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Equal(jobId, jobInfos.First().Id);
        }

        [Fact]
        public async Task GivenCreatedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenCreatedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned;

            string[] definitions = { "job1", "job2" };
            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                definitions,
                0,
                false,
                false,
                CancellationToken.None)).ToList();

            // check job info
            Assert.Equal(2, jobInfos.Count);

            JobInfo? jobInfo =
                await _azureStorageJobQueueClient.GetJobByIdAsync(
                    queueType,
                    jobInfos.First().Id,
                    true,
                    CancellationToken.None);
            Assert.Contains(jobInfo.Definition, definitions);

            definitions = new[] { "job1", "job2" };
            jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                definitions,
                0,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Equal(2, jobInfos.Count);
            List<long>? ids = new List<long> { jobInfos.First().Id, jobInfos.Last().Id };
            Assert.Contains(jobInfo.Id, ids);
            IEnumerable<JobInfo> retrievedJobInfos = await _azureStorageJobQueueClient.GetJobByGroupIdAsync(queueType, 0, true, CancellationToken.None);
            Assert.Equal(2, retrievedJobInfos.Count());
        }

        [Fact]
        public async Task GivenRunningJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenRunningJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned;

            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Single(jobInfos);

            JobInfo? jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.Equal(JobStatus.Running, jobInfo1.Status);

            // Enqueue again
            jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Equal(jobInfo1.Id, jobInfos.First().Id);

            // jobInfo1 should be the same as jobInfo2
            JobInfo? jobInfo2 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo1.Id, false, CancellationToken.None);

            Assert.Equal(JobStatus.Running, jobInfo2.Status);
            Assert.Equal(jobInfo1.CreateDate, jobInfo2.CreateDate);
            Assert.Equal(jobInfo1.HeartbeatDateTime, jobInfo2.HeartbeatDateTime);
            Assert.Equal(jobInfo1.Version, jobInfo2.Version);

            // the message is invisible
            Assert.Null(await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
        }

        [Fact]
        public async Task GivenFinishedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenFinishedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned;

            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Equal(3, jobInfos.Count);

            JobInfo? jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            JobInfo? jobInfo2 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            JobInfo? jobInfo3 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.Equal(JobStatus.Running, jobInfo1.Status);
            Assert.Equal(JobStatus.Running, jobInfo2.Status);
            Assert.Equal(JobStatus.Running, jobInfo3.Status);

            // the job1 is failed
            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for critical error";
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo1, false, CancellationToken.None);

            // the job2 is completed
            jobInfo2.Status = JobStatus.Completed;
            jobInfo2.Result = "OK";
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);

            // the job3 is cancelled
            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfo3.Id, CancellationToken.None);

            // Enqueue again, should return the existing one
            List<long> jobIds = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).Select(jobInfo => jobInfo.Id).ToList();

            Assert.Equal(3, jobIds.Count);
            Assert.Contains(jobInfo1.Id, jobIds);
            Assert.Contains(jobInfo2.Id, jobIds);
            Assert.Contains(jobInfo3.Id, jobIds);

            JobInfo? newJobInfo1 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo1.Id, false, CancellationToken.None);
            Assert.Equal(jobInfo1.Status, newJobInfo1.Status);
            JobInfo? newJobInfo2 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo2.Id, false, CancellationToken.None);
            Assert.Equal(jobInfo2.Status, newJobInfo2.Status);
            JobInfo? newJobInfo3 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo3.Id, false, CancellationToken.None);
            Assert.Equal(jobInfo3.Status, newJobInfo3.Status);

            // the message of job3 is invisible
            Assert.Null(await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));

            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));

            // the message of job3 is visible again
            Assert.NotNull(await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));

            // the message of job1 and job2 are deleted, so there is only one message
            Assert.Null(await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
        }

        [Fact]
        public async Task GivenEnqueueFailed_WhenEnqueueJobAgain_ThenContinueToEnqueue()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenEnqueueFailed_WhenEnqueueJobAgain_ThenContinueToEnqueue;

            // insert job info entity and job lock entity for job1
            (JobInfo? jobInfo, TableEntity? jobLockEntity) = await EnqueueStepBySteps(1001, queueType, 1);

            // enqueue job1 again
            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1001", },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Single(jobInfos);
            Assert.Equal(jobInfo.Id, jobInfos.First().Id);

            Assert.Null(jobLockEntity[JobLockEntityProperties.JobMessageId]);
            Assert.Null(jobLockEntity[JobLockEntityProperties.JobMessagePopReceipt]);

            await CheckJob(jobInfo, jobLockEntity);
            JobInfo? dequeuedJobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(jobInfo.QueueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.Equal(jobInfo.Id, dequeuedJobInfo.Id);
            await CleanStorage();

            // insert job reverse index entity for job2
            (jobInfo, jobLockEntity) = await EnqueueStepBySteps(1002, queueType, 2);

            // enqueue job2 again
            jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1002" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Single(jobInfos);
            Assert.Equal(jobInfo.Id, jobInfos.First().Id);

            Assert.Null(jobLockEntity[JobLockEntityProperties.JobMessageId]);
            Assert.Null(jobLockEntity[JobLockEntityProperties.JobMessagePopReceipt]);

            await CheckJob(jobInfo, jobLockEntity);
            dequeuedJobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(jobInfo.QueueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.Equal(jobInfo.Id, dequeuedJobInfo.Id);
            await CleanStorage();

            // send message for job3
            (jobInfo, jobLockEntity) = await EnqueueStepBySteps(1003, queueType, 3);

            // enqueue job3 again
            jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1003" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Single(jobInfos);
            Assert.Equal(jobInfo.Id, jobInfos.First().Id);

            Assert.Null(jobLockEntity[JobLockEntityProperties.JobMessageId]);
            Assert.Null(jobLockEntity[JobLockEntityProperties.JobMessagePopReceipt]);

            await CheckJob(jobInfo, jobLockEntity);

            // the first message is invalid
            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.DequeueAsync(jobInfo.QueueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
            Assert.EndsWith("the message id is inconsistent with the one in the table entity.", exception.Message);

            // the second message is valid
            dequeuedJobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(jobInfo.QueueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.Equal(jobInfo.Id, dequeuedJobInfo.Id);
        }

        // Dequeue
        [Fact]
        public async Task GivenJobsEnqueue_WhenDequeue_ThenAllJobsShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsEnqueue_WhenDequeue_ThenAllJobsShouldBeReturned;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);
            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job2" },
                null,
                false,
                false,
                CancellationToken.None);

            List<string> definitions = new List<string>();
            JobInfo jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            definitions.Add(jobInfo1.Definition);
            JobInfo jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            definitions.Add(jobInfo2.Definition);
            Assert.Null(
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));

            Assert.Contains("job1", definitions);
            Assert.Contains("job2", definitions);
        }

        [Fact]
        public async Task GivenJobNotHeartbeat_WhenDequeue_ThenJobShouldBeReturnedAgain()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobNotHeartbeat_WhenDequeue_ThenJobShouldBeReturnedAgain;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            JobInfo jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            // the heartbeatTimeoutSec is used as the message visibility timeout
            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));

            // without keeping alive, the job should be dequeued again
            JobInfo jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.Equal(jobInfo1.Id, jobInfo2.Id);
            Assert.True(jobInfo1.Version < jobInfo2.Version);
            Assert.Null(
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobKeepPutHeartbeat_WhenDequeue_ThenJobShouldNotBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobKeepPutHeartbeat_WhenDequeue_ThenJobShouldNotBeReturned;

            List<JobInfo> jobs = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { $"job1-{queueType}" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobs);

            JobInfo jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.Equal(jobInfo1.Id, jobs.First().Id);

            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));
            bool cancelRequested = await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None);
            Assert.False(cancelRequested);

            // after keeping alive, the job should not be returned
            Assert.Null(
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobKeepPutHeartbeatWithResult_WhenDequeue_ThenJobWithResultShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenJobKeepPutHeartbeatWithResult_WhenDequeue_ThenJobWithResultShouldBeReturned;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            JobInfo jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo1.QueueType = queueType;
            jobInfo1.Result = "current-result";
            await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None);
            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));
            JobInfo jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.Equal(jobInfo1.Result, jobInfo2.Result);
        }

        [Fact]
        public async Task GivenEmptyQueue_WhenDequeue_ThenNoResultShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenEmptyQueue_WhenDequeue_ThenNoResultShouldBeReturned;

            Assert.Null(
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobsEnqueue_WhenDequeueConcurrently_ThenCorrectResultShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsEnqueue_WhenDequeueConcurrently_ThenCorrectResultShouldBeReturned;

            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            List<Task<JobInfo>>? tasks = new List<Task<JobInfo>>
            {
                _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None),
                _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None),
            };

            JobInfo[] result = await Task.WhenAll(tasks);
            if (result[0] == null)
            {
                Assert.Equal(jobInfos.First().Id, result[1].Id);
            }
            else
            {
                Assert.Null(result[1]);
                Assert.Equal(jobInfos.First().Id, result[0].Id);
            }

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None);

            tasks = new List<Task<JobInfo>>
            {
                _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None),
                _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None),
            };

            result = await Task.WhenAll(tasks);
            List<string>? definitions = new List<string> { result[0].Definition, result[1].Definition };

            Assert.Contains("job2", definitions);
            Assert.Contains("job3", definitions);
        }

        [Fact]
        public async Task GivenFinishedJobs_WhenDequeue_ThenNoResultShouldBeReturned()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenFinishedJobs_WhenDequeue_ThenNoResultShouldBeReturned;

            // enqueue jobs
            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Equal(3, jobInfos.Count);

            JobInfo? jobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo.Status = JobStatus.Completed;

            var jobInfoEntity = ((AzureStorageJobInfo)jobInfo).ToTableEntity();
            await _azureJobInfoTableClient.UpdateEntityAsync(jobInfoEntity, ETag.All, cancellationToken: CancellationToken.None);

            jobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo.Status = JobStatus.Failed;

            jobInfoEntity = ((AzureStorageJobInfo)jobInfo).ToTableEntity();
            await _azureJobInfoTableClient.UpdateEntityAsync(jobInfoEntity, ETag.All, cancellationToken: CancellationToken.None);

            jobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo.Status = JobStatus.Cancelled;

            jobInfoEntity = ((AzureStorageJobInfo)jobInfo).ToTableEntity();
            await _azureJobInfoTableClient.UpdateEntityAsync(jobInfoEntity, ETag.All, cancellationToken: CancellationToken.None);

            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));

            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.DequeueAsync(jobInfo.QueueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
            Assert.EndsWith("the job status is Completed.", exception.Message);

            exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.DequeueAsync(jobInfo.QueueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
            Assert.EndsWith("the job status is Failed.", exception.Message);

            exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.DequeueAsync(jobInfo.QueueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
            Assert.EndsWith("the job status is Cancelled.", exception.Message);

            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));

            // the message is deleted
            Assert.Null(
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobCancelRequested_WhenDequeue_ThenTheJobShouldBeReturned()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenJobCancelRequested_WhenDequeue_ThenTheJobShouldBeReturned;

            // enqueue jobs
            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfos);

            JobInfo? jobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            // cancel running job, only cancelRequest is set to true
            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfo.Id, CancellationToken.None);
            jobInfo = await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo.Id, false, CancellationToken.None);
            Assert.Equal(JobStatus.Running, jobInfo.Status);
            Assert.True(jobInfo.CancelRequested);
            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));

            // re-dequeue again
            jobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.NotNull(jobInfo);
            Assert.Equal(jobInfos.First().Id, jobInfo.Id);
            Assert.Equal(JobStatus.Running, jobInfo.Status);
            Assert.True(jobInfo.CancelRequested);
        }

        [Fact]
        public async Task GivenNullMessageInTable_WhenDequeue_ThenTheMessageWillBeSkipped()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenNullMessageInTable_WhenDequeue_ThenTheMessageWillBeSkipped;

            _ = await EnqueueStepBySteps(1, queueType, 3);

            JobInfo? jobInfo = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.Null(jobInfo);
        }

        [Fact]
        public async Task GivenInvalidMessage_WhenDequeue_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenInvalidMessage_WhenDequeue_ThenTheExceptionShouldBeThrown;

            await _azureJobMessageQueueClient.SendMessageAsync("invalid message", CancellationToken.None);

            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
            Assert.Contains("failed to deserialize message", exception.Message);
        }

        [Fact]
        public async Task GivenMessageWithoutTableEntity_WhenDequeue_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenMessageWithoutTableEntity_WhenDequeue_ThenTheExceptionShouldBeThrown;

            await _azureJobMessageQueueClient.SendMessageAsync(new JobMessage(AzureStorageKeyProvider.JobInfoPartitionKey(0, 0), AzureStorageKeyProvider.JobInfoRowKey(0, 1), AzureStorageKeyProvider.JobLockRowKey("hash")).ToString(), CancellationToken.None);

            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
            Assert.Contains("failed to acquire job entity from table for message", exception.Message);
        }

        [Fact]
        public async Task GivenMessageInconsistentWithTableEntity_WhenDequeue_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenMessageInconsistentWithTableEntity_WhenDequeue_ThenTheExceptionShouldBeThrown;

            // enqueue fails at first time, send the message while fail to update job lock entity
            _ = await EnqueueStepBySteps(1, queueType, 3);

            // re-enqueue
            List<JobInfo> jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfos);

            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
            Assert.Contains("the message id is inconsistent with the one in the table entity.", exception.Message);

            JobInfo jobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.Equal(jobInfos.First().Definition, jobInfo.Definition);
        }

        // GetJobById
        [Fact]
        public async Task GivenJobId_WhenGetJobById_ThenTheJobShouldBeReturned()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenJobId_WhenGetJobById_ThenTheJobShouldBeReturned;

            // enqueue jobs
            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfos);

            JobInfo? jobInfo1 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfos.First().Id, true, CancellationToken.None);
            Assert.Equal("job1", jobInfo1.Definition);
            Assert.Equal(jobInfos.First().Id, jobInfo1.Id);
        }

        [Fact]
        public async Task GivenJobsEnqueue_WhenGetJobWithReturnDefinitionFalse_ThenTheDefinitionShouldNotBeReturned()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenJobsEnqueue_WhenGetJobWithReturnDefinitionFalse_ThenTheDefinitionShouldNotBeReturned;

            // enqueue jobs
            List<JobInfo> jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfos);

            JobInfo jobInfo1 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfos.First().Id, false, CancellationToken.None);
            Assert.Equal(jobInfos.First().Id, jobInfo1.Id);
            Assert.Null(jobInfo1.Definition);
        }

        [Fact]
        public async Task GivenJobId_WhenGetJobByIdConcurrently_ThenTheJobShouldBeReturned()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenJobId_WhenGetJobByIdConcurrently_ThenTheJobShouldBeReturned;

            // enqueue jobs
            List<JobInfo> jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfos);

            Task<JobInfo> task1 = _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfos.First().Id, true, CancellationToken.None);
            Task<JobInfo> task2 = _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfos.First().Id, true, CancellationToken.None);
            await Task.WhenAll(task1, task2);

            JobInfo job1 = task1.Result;
            JobInfo job2 = task2.Result;

            Assert.Equal(jobInfos.First().Id, job1.Id);
            Assert.Equal(job1.Id, job2.Id);
            Assert.Equal("job1", job1.Definition);
            Assert.Equal("job1", job2.Definition);
        }

        [Fact]
        public async Task GivenNotExistJobId_WhenGetJobById_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenNotExistJobId_WhenGetJobById_ThenTheExceptionShouldBeThrown;

            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, 1, false, CancellationToken.None));
            Assert.Equal("Failed to get job reverse index entity by id 1, the job reverse index entity does not exist.", exception.Message);
        }

        // GetJobsByIds
        [Fact]
        public async Task GivenJobsEnqueue_WhenGetJobsByIds_ThenTheJobsShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsEnqueue_WhenGetJobsByIds_ThenTheJobsShouldBeReturned;

            // enqueue jobs
            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Equal(3, jobInfos.Count);

            List<JobInfo>? retrievedJobInfos = (await _azureStorageJobQueueClient.GetJobsByIdsAsync(
                queueType,
                new[] { jobInfos[0].Id, jobInfos[2].Id },
                false,
                CancellationToken.None)).ToList();

            Assert.Equal(2, retrievedJobInfos.Count);
            List<long>? ids = new List<long> { retrievedJobInfos[0].Id, retrievedJobInfos[1].Id };
            Assert.Contains(jobInfos[0].Id, ids);
            Assert.Contains(jobInfos[2].Id, ids);
        }

        [Fact]
        public async Task GivenNotExistJobId_WhenGetJobsByIds_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenNotExistJobId_WhenGetJobsByIds_ThenTheExceptionShouldBeThrown;

            // enqueue jobs
            List<JobInfo> jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Equal(3, jobInfos.Count);

            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.GetJobsByIdsAsync(queueType, new[] { jobInfos[0].Id, 10, jobInfos[2].Id, 11 }, false, CancellationToken.None));
            Assert.Contains("Failed to get jobs by ids", exception.Message);
        }

        // GetJobByGroupId
        [Fact]
        public async Task GivenGroupJobs_WhenGetJobByGroupId_ThenTheCorrectJobsShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenGroupJobs_WhenGetJobByGroupId_ThenTheCorrectJobsShouldBeReturned;

            // enqueue jobs
            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                1,
                false,
                false,
                CancellationToken.None);
            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job4", "job5" },
                2,
                false,
                false,
                CancellationToken.None);

            List<JobInfo>? retrievedJobInfos =
                (await _azureStorageJobQueueClient.GetJobByGroupIdAsync(queueType, 2, true, CancellationToken.None))
                .ToList();

            Assert.Equal(2, retrievedJobInfos.Count);
            List<string>? definitions = new List<string>
                { retrievedJobInfos.First().Definition, retrievedJobInfos.Last().Definition };
            Assert.Contains("job4", definitions);
            Assert.Contains("job5", definitions);
        }

        [Fact]
        public async Task GivenNoJobInGroup_WhenGetJobByGroupId_ThenNoJobShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType =
                (byte)TestQueueType.GivenNoJobInGroup_WhenGetJobByGroupId_ThenNoJobShouldBeReturned;

            // enqueue jobs
            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                1,
                false,
                false,
                CancellationToken.None);

            List<JobInfo> retrievedJobInfos =
                (await _azureStorageJobQueueClient.GetJobByGroupIdAsync(queueType, 2, true, CancellationToken.None))
                .ToList();

            Assert.Empty(retrievedJobInfos);
        }

        [Fact]
        public async Task GivenGroupJobs_WhenGetJobByGroupIdWithReturnDefinitionFalse_ThenTheDefinitionShouldNotBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenGroupJobs_WhenGetJobByGroupIdWithReturnDefinitionFalse_ThenTheDefinitionShouldNotBeReturned;

            // enqueue jobs
            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                1,
                false,
                false,
                CancellationToken.None);

            List<JobInfo> retrievedJobInfos =
                (await _azureStorageJobQueueClient.GetJobByGroupIdAsync(queueType, 1, false, CancellationToken.None))
                .ToList();

            Assert.Equal(3, retrievedJobInfos.Count);

            foreach (JobInfo jobInfo in retrievedJobInfos)
            {
                Assert.Null(jobInfo.Definition);
            }
        }

        // KeepAliveJob
        [Fact]
        public async Task GivenJobWithResult_WhenKeepAlive_ThenTheResultShouldBeUpdated()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenJobWithResult_WhenKeepAlive_ThenTheResultShouldBeUpdated;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            JobInfo jobInfo =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.Empty(jobInfo.Result);
            TableEntity? jobLockEntity1 = (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(
                AzureStorageKeyProvider.JobInfoPartitionKey(queueType, jobInfo.GroupId),
                AzureStorageKeyProvider.JobLockRowKey(((AzureStorageJobInfo)jobInfo).JobIdentifier()),
                cancellationToken: CancellationToken.None)).Value;

            jobInfo.Result = "current-result";
            Assert.False(await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo, CancellationToken.None));

            JobInfo retrievedJob = await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo.Id, true, CancellationToken.None);

            Assert.Equal("current-result", retrievedJob.Result);
            Assert.Equal(JobStatus.Running, retrievedJob.Status);
            Assert.True(jobInfo.HeartbeatDateTime < retrievedJob.HeartbeatDateTime);

            TableEntity? jobLockEntity2 = (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(
                AzureStorageKeyProvider.JobInfoPartitionKey(queueType, jobInfo.GroupId),
                AzureStorageKeyProvider.JobLockRowKey(((AzureStorageJobInfo)jobInfo).JobIdentifier()),
                cancellationToken: CancellationToken.None)).Value;

            Assert.Equal(jobLockEntity1.GetString(JobLockEntityProperties.JobMessageId), jobLockEntity2.GetString(JobLockEntityProperties.JobMessageId));
            Assert.NotEqual(jobLockEntity1.GetString(JobLockEntityProperties.JobMessagePopReceipt), jobLockEntity2.GetString(JobLockEntityProperties.JobMessagePopReceipt));
        }

        [Fact]
        public async Task GivenRunningJobCancelled_WhenKeepAlive_ThenCancelRequestedShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenRunningJobCancelled_WhenKeepAlive_ThenCancelRequestedShouldBeReturned;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            JobInfo jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo1.QueueType = queueType;
            jobInfo1.Result = "current-result";
            Assert.False(await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
            await _azureStorageJobQueueClient.CancelJobByGroupIdAsync(queueType, jobInfo1.GroupId, CancellationToken.None);
            Assert.True(await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
        }

        [Fact]
        public async Task GivenRunningJob_WhenKeepAliveFailed_ThenTheJobShouldBeDequeuedAgain()
        {
            await CleanStorage();
            const byte queueType =
                (byte)TestQueueType.GivenRunningJob_WhenKeepAliveFailed_ThenTheJobShouldBeDequeuedAgain;

            // enqueue jobs
            _ = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                1,
                false,
                false,
                CancellationToken.None)).ToList();

            JobInfo jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            var jobInfoEntity = ((AzureStorageJobInfo)jobInfo1).ToTableEntity();

            TableEntity? jobLockEntity = (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(
                jobInfoEntity.PartitionKey,
                AzureStorageKeyProvider.JobLockRowKey(((AzureStorageJobInfo)jobInfo1).JobIdentifier()),
                cancellationToken: CancellationToken.None)).Value;

            // the message is updated, while table entity isn't update
            await _azureJobMessageQueueClient.UpdateMessageAsync(
                jobLockEntity.GetString(JobLockEntityProperties.JobMessageId),
                jobLockEntity.GetString(JobLockEntityProperties.JobMessagePopReceipt),
                visibilityTimeout: TimeSpan.FromSeconds((long)jobInfoEntity[JobInfoEntityProperties.HeartbeatTimeoutSec]),
                cancellationToken: CancellationToken.None);

            // keep alive should throw exception
            var exception = await Assert.ThrowsAsync<JobNotExistException>(async () => await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
            Assert.Contains("the job message with the specified pop receipt is not found", exception.Message);

            // the message is still invisible
            Assert.Null(await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));

            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));

            // keep alive should still throw exception
            exception = await Assert.ThrowsAsync<JobNotExistException>(async () => await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
            Assert.Contains("the job message with the specified pop receipt is not found", exception.Message);

            // re-dequeue
            JobInfo jobInfo2 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.NotNull(jobInfo2);
            Assert.Equal(jobInfo1.Id, jobInfo2.Id);

            // keep alive should throw JobNotExistException for jobInfo1 as the version doesn't match
            exception = await Assert.ThrowsAsync<JobNotExistException>(async () => await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
            Assert.Equal($"Job {jobInfo1.Id} precondition failed, version does not match.", exception.Message);

            // keep alive successfully for jobInfo2
            bool shouldCancel = await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo2, CancellationToken.None);
            Assert.False(shouldCancel);

            // complete jobInfo1 should throw JobNotExistException
            jobInfo1.Status = JobStatus.Completed;
            await Assert.ThrowsAsync<JobNotExistException>(async () => await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo1, true, CancellationToken.None));

            // complete jobInfo2 successfully
            jobInfo2.Status = JobStatus.Completed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);
        }

        // CancelJobByGroupId
        [Fact]
        public async Task GivenGroupJobs_WhenCancelJobByGroupId_ThenAllJobsShouldBeCancelled()
        {
            await CleanStorage();

            const byte queueType =
                (byte)TestQueueType.GivenGroupJobs_WhenCancelJobByGroupId_ThenAllJobsShouldBeCancelled;

            List<JobInfo> jobGroup1 = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                1,
                false,
                false,
                CancellationToken.None)).ToList();

            List<JobInfo> jobGroup2 = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job4", "job5" },
                2,
                false,
                false,
                CancellationToken.None)).ToList();

            await _azureStorageJobQueueClient.CancelJobByGroupIdAsync(queueType, 1, CancellationToken.None);

            foreach (JobInfo jobInfo in jobGroup2)
            {
                JobInfo retrievedJob = await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo.Id, false, CancellationToken.None);
                Assert.False(retrievedJob.CancelRequested);
                Assert.Equal(JobStatus.Created, retrievedJob.Status);
            }

            foreach (JobInfo jobInfo in jobGroup1)
            {
                JobInfo retrievedJob = await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo.Id, false, CancellationToken.None);
                Assert.True(retrievedJob.CancelRequested);
                Assert.Equal(JobStatus.Cancelled, retrievedJob.Status);
            }
        }

        [Fact]
        public async Task GivenJobsWithDifferentStatus_WhenCancelJobByGroupId_ThenTheStatusShouldBeSetCorrectly()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsWithDifferentStatus_WhenCancelJobByGroupId_ThenTheStatusShouldBeSetCorrectly;

            // enqueue jobs
            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3", "job4" },
                1,
                false,
                false,
                CancellationToken.None)).ToList();

            JobInfo? jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            JobInfo? jobInfo2 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            JobInfo? jobInfo3 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo2.Status = JobStatus.Failed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);

            jobInfo3.Status = JobStatus.Completed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo3, false, CancellationToken.None);

            await _azureStorageJobQueueClient.CancelJobByGroupIdAsync(queueType, 1, CancellationToken.None);

            List<JobInfo>? retrievedJobInfos = (await _azureStorageJobQueueClient.GetJobsByIdsAsync(queueType, jobInfos.Select(j => j.Id).ToArray(), false, CancellationToken.None)).ToList();

            Assert.Empty(retrievedJobInfos.Where(j => !j.CancelRequested));

            // job1 is running
            Assert.Equal(JobStatus.Running, retrievedJobInfos.First(job => job.Id == jobInfo1.Id).Status);

            // job2 is failed
            Assert.Equal(JobStatus.Failed, retrievedJobInfos.First(job => job.Id == jobInfo2.Id).Status);

            // job3 is completed
            Assert.Equal(JobStatus.Completed, retrievedJobInfos.First(job => job.Id == jobInfo3.Id).Status);

            // job4 is created, its status will be changed to cancelled
            Assert.Equal(JobStatus.Cancelled, retrievedJobInfos.First(job => job.Id == jobInfos.First(jobInfo => jobInfo.Id != jobInfo1.Id && jobInfo.Id != jobInfo2.Id && jobInfo.Id != jobInfo3.Id).Id).Status);
        }

        [Fact]
        public async Task GivenNoJobInGroup_WhenCancelJobByGroupId_ThenNoExceptionShouldBeThrown()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenNoJobInGroup_WhenCancelJobByGroupId_ThenNoExceptionShouldBeThrown;

            // enqueue jobs
            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                1,
                false,
                false,
                CancellationToken.None);

            Exception? exception = await Record.ExceptionAsync(async () => await _azureStorageJobQueueClient.CancelJobByGroupIdAsync(queueType, 2, CancellationToken.None));

            Assert.Null(exception);
        }

        // CancelJobById
        [Fact]
        public async Task GivenGroupJobs_WhenCancelJobById_ThenOnlySingleJobShouldBeCancelled()
        {
            await CleanStorage();

            const byte queueType =
                (byte)TestQueueType.GivenGroupJobs_WhenCancelJobById_ThenOnlySingleJobShouldBeCancelled;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None);

            // get the job id of first message
            JobMessage? firstMessage = JobMessage.Parse((await _azureJobMessageQueueClient.PeekMessageAsync(CancellationToken.None)).Value.Body.ToString());
            Assert.NotNull(firstMessage);
            TableEntity? jobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(
                firstMessage?.PartitionKey,
                firstMessage?.RowKey,
                cancellationToken: CancellationToken.None)).Value;

            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, (long)jobInfoEntity[JobInfoEntityProperties.Id], CancellationToken.None);
            Assert.Equal(
                JobStatus.Cancelled,
                (await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, (long)jobInfoEntity[JobInfoEntityProperties.Id], false, CancellationToken.None)).Status);

            // job1 is cancelled
            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
            Assert.EndsWith("the job status is Cancelled.", exception.Message);

            JobInfo jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            JobInfo jobInfo3 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.False(jobInfo2.CancelRequested);
            Assert.False(jobInfo3.CancelRequested);

            Assert.Null(await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobsWithDifferentStatus_WhenCancelJobById_ThenTheStatusShouldBeSetCorrectly()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsWithDifferentStatus_WhenCancelJobById_ThenTheStatusShouldBeSetCorrectly;

            // enqueue jobs
            List<JobInfo>? jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3", "job4" },
                1,
                false,
                false,
                CancellationToken.None)).ToList();

            JobInfo? jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            JobInfo? jobInfo2 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            JobInfo? jobInfo3 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo2.Status = JobStatus.Failed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);

            jobInfo3.Status = JobStatus.Completed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo3, false, CancellationToken.None);

            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfos[0].Id, CancellationToken.None);
            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfos[1].Id, CancellationToken.None);
            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfos[2].Id, CancellationToken.None);
            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfos[3].Id, CancellationToken.None);

            List<JobInfo>? retrievedJobInfos = (await _azureStorageJobQueueClient.GetJobsByIdsAsync(queueType, jobInfos.Select(j => j.Id).ToArray(), false, CancellationToken.None)).ToList();

            Assert.Empty(retrievedJobInfos.Where(j => !j.CancelRequested));

            // job1 is running
            Assert.Equal(JobStatus.Running, retrievedJobInfos.First(job => job.Id == jobInfo1.Id).Status);

            // job2 is failed
            Assert.Equal(JobStatus.Failed, retrievedJobInfos.First(job => job.Id == jobInfo2.Id).Status);

            // job3 is completed
            Assert.Equal(JobStatus.Completed, retrievedJobInfos.First(job => job.Id == jobInfo3.Id).Status);

            // job4 is created, its status will be changed to cancelled
            Assert.Equal(JobStatus.Cancelled, retrievedJobInfos.First(job => job.Id == jobInfos.First(jobInfo => jobInfo.Id != jobInfo1.Id && jobInfo.Id != jobInfo2.Id && jobInfo.Id != jobInfo3.Id).Id).Status);
        }

        [Fact]
        public async Task GivenNoExistJob_WhenCancelJobById_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenNoExistJob_WhenCancelJobById_ThenTheExceptionShouldBeThrown;

            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, 1, CancellationToken.None));
            Assert.Equal("Failed to get job reverse index entity by id 1, the job reverse index entity does not exist.", exception.Message);
        }

        // CompleteJob
        [Fact]
        public async Task GivenGroupJobs_WhenCompleteJob_ThenJobsShouldBeCompleted()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenGroupJobs_WhenCompleteJob_ThenJobsShouldBeCompleted;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2" },
                null,
                false,
                false,
                CancellationToken.None);

            JobInfo jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            JobInfo jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.Equal(JobStatus.Running, jobInfo1.Status);
            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for critical error";
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo1, false, CancellationToken.None);
            JobInfo jobInfo =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo1.Id, false, CancellationToken.None);
            Assert.Equal(JobStatus.Failed, jobInfo.Status);
            Assert.Equal(jobInfo1.Result, jobInfo.Result);

            jobInfo2.Status = JobStatus.Completed;
            jobInfo2.Result = "Completed";
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);
            jobInfo = await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo2.Id, false, CancellationToken.None);
            Assert.Equal(JobStatus.Completed, jobInfo.Status);
            Assert.Equal(jobInfo2.Result, jobInfo.Result);
        }

        [Fact]
        public async Task GivenGroupJobs_WhenOneJobFailedAndRequestCancellation_ThenAllJobsShouldBeCancelled()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenGroupJobs_WhenOneJobFailedAndRequestCancellation_ThenAllJobsShouldBeCancelled;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None);

            JobInfo jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for cancellation";

            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo1, true, CancellationToken.None);
            Assert.True(
                (await _azureStorageJobQueueClient.GetJobByGroupIdAsync(queueType, jobInfo1.GroupId, false, CancellationToken.None)).All(
                    t => t.Status is JobStatus.Cancelled or JobStatus.Failed));
        }

        [Fact]
        public async Task GivenCancelledJobs_WhenCompleteJob_ThenTheJobStatusShouldBeCorrect()
        {
            await CleanStorage();

            const byte queueType =
                (byte)TestQueueType.GivenCancelledJobs_WhenCompleteJob_ThenTheJobStatusShouldBeCorrect;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None);

            JobInfo jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            JobInfo jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            await _azureStorageJobQueueClient.CancelJobByGroupIdAsync(queueType, jobInfo1.GroupId, CancellationToken.None);
            Assert.True(
                (await _azureStorageJobQueueClient.GetJobByGroupIdAsync(queueType, jobInfo1.GroupId, false, CancellationToken.None)).All(
                    t => t.Status == JobStatus.Cancelled || (t.Status == JobStatus.Running && t.CancelRequested)));
            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for cancellation";
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo1, false, CancellationToken.None);
            JobInfo jobInfo =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo1.Id, false, CancellationToken.None);
            Assert.Equal(JobStatus.Failed, jobInfo.Status);
            Assert.Equal(jobInfo1.Result, jobInfo.Result);

            jobInfo2.Status = JobStatus.Completed;
            jobInfo2.Result = "Completed";
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);
            jobInfo = await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo2.Id, false, CancellationToken.None);
            Assert.Equal(JobStatus.Cancelled, jobInfo.Status);
            Assert.Equal(jobInfo2.Result, jobInfo.Result);
        }

        private async Task CleanStorage()
        {
            await _azureJobMessageQueueClient.ClearMessagesAsync();
        }

        private async Task<Tuple<JobInfo, TableEntity>> EnqueueStepBySteps(long jobId, byte queueType, int steps)
        {
            var jobInfo = new AzureStorageJobInfo()
            {
                Id = jobId,
                QueueType = queueType,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = $"job{jobId}",
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };
            var jobInfoEntity = jobInfo.ToTableEntity();
            var jobLockEntity = new TableEntity(jobInfoEntity.PartitionKey, AzureStorageKeyProvider.JobLockRowKey(jobInfo.JobIdentifier()))
            {
                { JobLockEntityProperties.JobInfoEntityRowKey, jobInfoEntity.RowKey },
            };

            IEnumerable<TableTransactionAction> transactionAddActions = new List<TableTransactionAction>
            {
                new TableTransactionAction(TableTransactionActionType.Add, jobInfoEntity),
                new TableTransactionAction(TableTransactionActionType.Add, jobLockEntity),
            };

            _ = await _azureJobInfoTableClient.SubmitTransactionAsync(transactionAddActions, CancellationToken.None);

            if (steps == 1)
            {
                return new Tuple<JobInfo, TableEntity>(jobInfo, jobLockEntity);
            }

            var reverseIndexEntity2 = new JobReverseIndexEntity
            {
                PartitionKey = AzureStorageKeyProvider.JobReverseIndexPartitionKey(queueType, (long)jobInfoEntity[JobInfoEntityProperties.Id]),
                RowKey = AzureStorageKeyProvider.JobReverseIndexRowKey(queueType, (long)jobInfoEntity[JobInfoEntityProperties.Id]),
                JobInfoEntityPartitionKey = jobInfoEntity.PartitionKey,
                JobInfoEntityRowKey = jobInfoEntity.RowKey,
            };

            await _azureJobInfoTableClient.AddEntityAsync(reverseIndexEntity2, CancellationToken.None);

            if (steps == 2)
            {
                return new Tuple<JobInfo, TableEntity>(jobInfo, jobLockEntity);
            }

            await _azureJobMessageQueueClient.SendMessageAsync(new JobMessage(jobInfoEntity.PartitionKey, jobInfoEntity.RowKey, jobLockEntity.RowKey).ToString(), CancellationToken.None);
            return new Tuple<JobInfo, TableEntity>(jobInfo, jobLockEntity);
        }

        private async Task CheckJob(JobInfo jobInfo, TableEntity? jobLockEntity = null)
        {
            JobInfo? retrievedJobInfo =
                await _azureStorageJobQueueClient.GetJobByIdAsync(
                    jobInfo.QueueType,
                    jobInfo.Id,
                    true,
                    CancellationToken.None);
            Assert.Equal(jobInfo.Id, retrievedJobInfo.Id);
            Assert.Equal(jobInfo.Definition, retrievedJobInfo.Definition);

            // check table entity
            // job reverse index entity should exist
            string? reversePartitionKey = AzureStorageKeyProvider.JobReverseIndexPartitionKey(jobInfo.QueueType, retrievedJobInfo.Id);
            string? reverseRowKey = AzureStorageKeyProvider.JobReverseIndexRowKey(jobInfo.QueueType, retrievedJobInfo.Id);
            JobReverseIndexEntity? reverseIndexEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobReverseIndexEntity>(reversePartitionKey, reverseRowKey)).Value;
            Assert.NotNull(reverseIndexEntity);

            // job info entity should exist
            TableEntity? retrievedJobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(reverseIndexEntity.JobInfoEntityPartitionKey, reverseIndexEntity.JobInfoEntityRowKey)).Value;
            Assert.NotNull(retrievedJobInfoEntity);

            // job lock entity should exist
            string jobLockEntityRowKey =
                AzureStorageKeyProvider.JobLockRowKey(((AzureStorageJobInfo)retrievedJobInfo).JobIdentifier());
            TableEntity? retrievedJobLockEntity =
                (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(
                    retrievedJobInfoEntity.PartitionKey,
                    jobLockEntityRowKey)).Value;
            Assert.NotNull(retrievedJobLockEntity);
            Assert.Equal(retrievedJobInfoEntity.RowKey, retrievedJobLockEntity.GetString(JobLockEntityProperties.JobInfoEntityRowKey));
            Assert.NotNull(retrievedJobLockEntity.GetString(JobLockEntityProperties.JobMessageId));
            Assert.NotNull(retrievedJobLockEntity.GetString(JobLockEntityProperties.JobMessagePopReceipt));

            if (jobLockEntity != null)
            {
                Assert.Equal(jobLockEntity.PartitionKey, retrievedJobLockEntity.PartitionKey);
                Assert.Equal(jobLockEntity.RowKey, retrievedJobLockEntity.RowKey);
                Assert.Equal(jobLockEntity.GetString(JobLockEntityProperties.JobInfoEntityRowKey), retrievedJobLockEntity.GetString(JobLockEntityProperties.JobInfoEntityRowKey));
            }
        }
    }
}