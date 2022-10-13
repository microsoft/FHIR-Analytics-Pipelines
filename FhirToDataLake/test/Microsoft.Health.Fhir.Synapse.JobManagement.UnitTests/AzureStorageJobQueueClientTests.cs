// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure;
using Azure.Data.Tables;
using Azure.Storage.Queues;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.JobManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage;
using Microsoft.Health.JobManagement;
using Xunit;
using JobStatus = Microsoft.Health.JobManagement.JobStatus;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.UnitTests
{
    /// <summary>
    /// Use different queue type for integration test to avoid conflict
    /// </summary>
    internal enum TestQueueType : byte
    {
        GivenNewJobs_WhenEnqueueJobs_ThenCreatedJobsShouldBeReturned = 16,
        GivenNewJobsWithSameQueueType_WhenEnqueueWithForceOneActiveJobGroup_ThenSecondJobShouldNotBeEnqueued,
        GivenJobsWithSameDefinition_WhenEnqueue_ThenOnlyOneJobShouldBeEnqueued,
        GivenJobsWithSameDefinition_WhenEnqueueWithGroupId_ThenGroupIdShouldBeCorrect,
        GivenJobsEnqueue_WhenDequeue_ThenAllJobsShouldBeReturned,
        GivenJobKeepPutHeartbeat_WhenDequeue_ThenJobShouldNotBeReturned,
        GivenJobKeepPutHeartbeatWithResult_WhenDequeue_ThenJobWithResultShouldNotBeReturned,
        GivenRunningJobCancelled_WhenKeepHeartbeat_ThenCancelRequestedShouldBeReturned,
        GivenJobNotHeartbeat_WhenDequeue_ThenJobShouldBeReturnedAgain,
        GivenGroupJobs_WhenCompleteJob_ThenJobsShouldBeCompleted,
        GivenGroupJobs_WhenCancelJobsByGroupId_ThenAllJobsShouldBeCancelled,
        GivenGroupJobs_WhenCancelJobsById_ThenOnlySingleJobShouldBeCancelled,
        GivenGroupJobs_WhenOneJobFailedAndRequestCancellation_ThenAllJobsShouldBeCancelled,
        GivenJobsWithSameDefinition_WhenEnqueueConcurrently_ThenOnlyOneJobShouldBeEnqueued,
        GivenJobsWithSameDefinition_WhenEnqueueInABatch_ThenTheExceptionShouldBeThrown,
        GivenCreatedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned,
        GivenRunningJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned,
        GivenFinishedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned,
        GivenEnqueueFailed_WhenEnqueueJobAgain_ThenContinueToEnqueue,
        GivenJobsLargerThanTransactionLimitation_WhenEnqueue_ThenTheExceptionShouldBeThrown,
        GivenEmptyQueue_WhenDequeue_ThenNoResultShouldBeReturned,
        GivenJobsEnqueue_WhenDequeueConcurrently_ThenCorrectResultShouldBeReturned,
        GivenFinishedJobs_WhenDequeue_ThenNoResultShouldBeReturned,
        GivenJobCancelRequested_WhenDequeue_ThenTheJobShouldBeReturned,
        GivenNullMessageInTable_WhenDequeue_ThenTheMessageWillBeSkipped,
        GivenJobsEnqueue_WhenGetJobWithReturnDefinitionFalse_ThenTheDefinitionShouldNotBeReturned,
        GivenJobsEnqueue_WhenGetJobsByIds_ThenTheJobsShouldBeReturned,
        GivenGroupJobs_WhenGetJobByGroupId_ThenTheCorrectJobsShouldBeReturned,
        GivenJobsWithDifferentStatus_WhenCancelJobById_ThenTheStatusShouldBeSetCorrectly,
        GivenJobsWithDifferentStatus_WhenCancelJobByGroupId_ThenTheStatusShouldBeSetCorrectly,
        GivenRunningJob_WhenKeepAliveFailed_ThenTheJobShouldBeDequeuedAgain,
    }

    [Trait("Category", "JobManagementTests")]
    public class AzureStorageJobQueueClientTests
    {
        private readonly NullLogger<AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo>>
            _nullAzureStorageJobQueueClientLogger =
                NullLogger<AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo>>.Instance;

        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";
        private const string TestJobInfoTableName = "testjobinfotable";
        private const string TestJobInfoQueueName = "testjobinfoqueue";
        private const string TestWorkerName = "test-worker";
        private const int HeartbeatTimeoutSec = 2;

        private readonly AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo> _azureStorageJobQueueClient;
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
                new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()));

            _azureStorageJobQueueClient = new AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo>(
                azureStorageClientFactory,
                _nullAzureStorageJobQueueClientLogger);

            _azureStorageJobQueueClient.IsInitialized();
        }

        [Fact]
        public async Task GivenNewJobs_WhenEnqueueJobs_ThenCreatedJobsShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenNewJobs_WhenEnqueueJobs_ThenCreatedJobsShouldBeReturned;

            var definitions = new[] { "job1", "job2" };
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
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

            var jobInfo =
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

            await _azureStorageJobQueueClient.EnqueueAsync(queueType, new string[] { "job1" }, null, true, false, CancellationToken.None);

            // TODO: this field ForceOneActiveJobGroup isn't implemented
            // await Assert.ThrowsAsync<JobConflictException>(async () => await _azureStorageJobQueueClient.EnqueueAsync(queueType, new string[] { "job2" }, null, true, false, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobsWithSameDefinition_WhenEnqueue_ThenOnlyOneJobShouldBeEnqueued()
        {
            await CleanStorage();

            const byte queueType =
                (byte)TestQueueType.GivenJobsWithSameDefinition_WhenEnqueue_ThenOnlyOneJobShouldBeEnqueued;

            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfos);
            var jobId = jobInfos.First().Id;
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
        public async Task GivenJobsWithSameDefinition_WhenEnqueueWithGroupId_ThenGroupIdShouldBeCorrect()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenJobsWithSameDefinition_WhenEnqueueWithGroupId_ThenGroupIdShouldBeCorrect;

            long groupId = new Random().Next(int.MinValue, int.MaxValue);
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2" },
                groupId,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Equal(2, jobInfos.Count);
            Assert.Equal(groupId, jobInfos.First().GroupId);
            Assert.Equal(groupId, jobInfos.Last().GroupId);
            jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job3", "job4" },
                groupId,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Equal(2, jobInfos.Count);
            Assert.Equal(groupId, jobInfos.First().GroupId);
            Assert.Equal(groupId, jobInfos.Last().GroupId);
        }

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

            var definitions = new List<string>();
            var jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            definitions.Add(jobInfo1.Definition);
            var jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            definitions.Add(jobInfo2.Definition);
            Assert.Null(
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));

            Assert.Contains("job1", definitions);
            Assert.Contains("job2", definitions);
        }

        [Fact]
        public async Task GivenJobKeepPutHeartbeat_WhenDequeue_ThenJobShouldNotBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobKeepPutHeartbeat_WhenDequeue_ThenJobShouldNotBeReturned;

            var jobs = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { $"job1-{queueType}" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobs);

            var jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.Equal(jobInfo1.Id, jobs.First().Id);
            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));

            // without keep alive, the job should be dequeued again
            var jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.NotNull(jobInfo2);
            Assert.Equal(jobInfo1.Id, jobInfo2.Id);

            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));
            var cancelRequested = await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo2, CancellationToken.None);
            Assert.False(cancelRequested);

            // after keeping alive, the job should not be returned
            Assert.Null(
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobKeepPutHeartbeatWithResult_WhenDequeue_ThenJobWithResultShouldNotBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenJobKeepPutHeartbeatWithResult_WhenDequeue_ThenJobWithResultShouldNotBeReturned;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            var jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo1.QueueType = queueType;
            jobInfo1.Result = "current-result";
            await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None);
            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));
            var jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.Equal(jobInfo1.Result, jobInfo2.Result);
        }

        [Fact]
        public async Task GivenRunningJobCancelled_WhenKeepHeartbeat_ThenCancelRequestedShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenRunningJobCancelled_WhenKeepHeartbeat_ThenCancelRequestedShouldBeReturned;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            var jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo1.QueueType = queueType;
            jobInfo1.Result = "current-result";
            Assert.False(await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
            await _azureStorageJobQueueClient.CancelJobByGroupIdAsync(queueType, jobInfo1.GroupId, CancellationToken.None);
            Assert.True(await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
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

            var jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            // the heartbeatTimeoutSec is used as the message visibility timeout
            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));
            var jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.Equal(jobInfo1.Id, jobInfo2.Id);
            Assert.True(jobInfo1.Version < jobInfo2.Version);
            Assert.Null(
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
        }

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

            var jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            var jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.Equal(JobStatus.Running, jobInfo1.Status);
            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for cancellation";
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo1, false, CancellationToken.None);
            var jobInfo =
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
        public async Task GivenGroupJobs_WhenCancelJobsByGroupId_ThenAllJobsShouldBeCancelled()
        {
            await CleanStorage();

            const byte queueType =
                (byte)TestQueueType.GivenGroupJobs_WhenCancelJobsByGroupId_ThenAllJobsShouldBeCancelled;

            await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None);

            var jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            var jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            await _azureStorageJobQueueClient.CancelJobByGroupIdAsync(queueType, jobInfo1.GroupId, CancellationToken.None);
            Assert.True(
                (await _azureStorageJobQueueClient.GetJobByGroupIdAsync(queueType, jobInfo1.GroupId, false, CancellationToken.None)).All(
                    t => t.Status == JobStatus.Cancelled || (t.Status == JobStatus.Running && t.CancelRequested)));
            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for cancellation";
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo1, false, CancellationToken.None);
            var jobInfo =
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

        [Fact]
        public async Task GivenGroupJobs_WhenCancelJobsById_ThenOnlySingleJobShouldBeCancelled()
        {
            await CleanStorage();

            const byte queueType =
                (byte)TestQueueType.GivenGroupJobs_WhenCancelJobsById_ThenOnlySingleJobShouldBeCancelled;

            var jobs = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            // get the job id of first message
            var firstMessage = JobMessage.Parse((await _azureJobMessageQueueClient.PeekMessageAsync(CancellationToken.None)).Value.Body.ToString());
            Assert.NotNull(firstMessage);
            var jobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(
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

            var jobInfo2 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            var jobInfo3 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            Assert.False(jobInfo2.CancelRequested);
            Assert.False(jobInfo3.CancelRequested);

            Assert.Null(await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));
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

            var jobInfo1 =
                await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for critical error";

            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo1, true, CancellationToken.None);
            Assert.True(
                (await _azureStorageJobQueueClient.GetJobByGroupIdAsync(queueType, jobInfo1.GroupId, false, CancellationToken.None)).All(
                    t => t.Status is JobStatus.Cancelled or JobStatus.Failed));
        }

        [Fact]
        public async Task GivenJobsWithSameDefinition_WhenEnqueueConcurrently_ThenOnlyOneJobShouldBeEnqueued()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType
                .GivenJobsWithSameDefinition_WhenEnqueueConcurrently_ThenOnlyOneJobShouldBeEnqueued;

            var tasks = new List<Task<IEnumerable<JobInfo>>>();
            var task1 = _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            var task2 = _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None);

            tasks.Add(task1);
            tasks.Add(task2);

            var result = await Task.WhenAll(tasks);
            Assert.Single(result[0]);
            Assert.Single(result[1]);
            Assert.Equal(result[0].ToList().First().Id, result[1].ToList().First().Id);
        }

        [Fact]
        public async Task GivenJobsWithSameDefinition_WhenEnqueueInABatch_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsWithSameDefinition_WhenEnqueueInABatch_ThenTheExceptionShouldBeThrown;

            var exception = await Assert.ThrowsAsync<JobManagementException>(async () =>
                await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job1" },
                null,
                false,
                false,
                CancellationToken.None));
        }

        [Fact]
        public async Task GivenCreatedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenCreatedJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned;

            var definitions = new[] { "job1", "job2" };
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                definitions,
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            // check job info
            Assert.Equal(2, jobInfos.Count);

            var jobInfo =
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
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Equal(2, jobInfos.Count);
            var ids = new List<long> { jobInfos.First().Id, jobInfos.Last().Id };
            Assert.Contains(jobInfo.Id, ids);
        }

        [Fact]
        public async Task GivenRunningJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenRunningJob_WhenEnqueueJobAgain_ThenTheExistingOneShouldBeReturned;

            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Single(jobInfos);

            var jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

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
            var jobInfo2 =
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

            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Equal(3, jobInfos.Count);

            var jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            var jobInfo2 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            var jobInfo3 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

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
            var jobids = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).Select(jobInfo => jobInfo.Id).ToList();

            Assert.Equal(3, jobids.Count);
            Assert.Contains(jobInfo1.Id, jobids);
            Assert.Contains(jobInfo2.Id, jobids);
            Assert.Contains(jobInfo3.Id, jobids);

            var newJobInfo1 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo1.Id, false, CancellationToken.None);
            Assert.Equal(jobInfo1.Status, newJobInfo1.Status);
            var newJobInfo2 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfo2.Id, false, CancellationToken.None);
            Assert.Equal(jobInfo2.Status, newJobInfo2.Status);
            var newJobInfo3 =
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
            var (jobInfo, jobLockEntity) = await EnqueueStepBySteps(1001, queueType, 1);

            // enqueue job1 again
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
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
            var dequeuedJobInfo =
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

        [Fact]
        public async Task GivenJobsLargerThanTransactionLimitation_WhenEnqueue_ThenTheExceptionShouldBeThrown()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsLargerThanTransactionLimitation_WhenEnqueue_ThenTheExceptionShouldBeThrown;

            const int jobCnt = 51;
            var definitions = new List<string>();
            for (var i = 0; i < jobCnt; i++)
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

            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            var tasks = new List<Task<JobInfo>>
            {
                _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None),
                _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None),
            };

            var result = await Task.WhenAll(tasks);
            if (result[0] == null)
            {
                Assert.Equal(jobInfos.First().Id, result[1].Id);
            }
            else
            {
                Assert.Null(result[1]);
                Assert.Equal(jobInfos.First().Id, result[0].Id);
            }

            jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            tasks = new List<Task<JobInfo>>
            {
                _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None),
                _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None),
            };

            result = await Task.WhenAll(tasks);
            var definitions = new List<string> { result[0].Definition, result[1].Definition };

            Assert.Contains("job2", definitions);
            Assert.Contains("job3", definitions);
        }

        [Fact]
        public async Task GivenFinishedJobs_WhenDequeue_ThenNoResultShouldBeReturned()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenFinishedJobs_WhenDequeue_ThenNoResultShouldBeReturned;

            // enqueue jobs
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Equal(3, jobInfos.Count);

            var jobInfo =
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
        }

        [Fact]
        public async Task GivenJobCancelRequested_WhenDequeue_ThenTheJobShouldBeReturned()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenJobCancelRequested_WhenDequeue_ThenTheJobShouldBeReturned;

            // enqueue jobs
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfos);

            var jobInfo =
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

            var jobInfo = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.Null(jobInfo);
        }

        [Fact]
        public async Task GivenJobsEnqueue_WhenGetJobWithReturnDefinitionFalse_ThenTheDefinitionShouldNotBeReturned()
        {
            await CleanStorage();
            const byte queueType = (byte)TestQueueType.GivenJobsEnqueue_WhenGetJobWithReturnDefinitionFalse_ThenTheDefinitionShouldNotBeReturned;

            // enqueue jobs
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfos);

            var jobInfo1 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfos.First().Id, true, CancellationToken.None);
            Assert.Equal("job1", jobInfo1.Definition);

            var jobInfo2 =
                await _azureStorageJobQueueClient.GetJobByIdAsync(queueType, jobInfos.First().Id, false, CancellationToken.None);
            Assert.Equal(jobInfos.First().Id, jobInfo2.Id);
            Assert.Null(jobInfo2.Definition);
        }

        [Fact]
        public async Task GivenJobsEnqueue_WhenGetJobsByIds_ThenTheJobsShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsEnqueue_WhenGetJobsByIds_ThenTheJobsShouldBeReturned;

            // enqueue jobs
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3" },
                null,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Equal(3, jobInfos.Count);

            var retrievedJobInfos = (await _azureStorageJobQueueClient.GetJobsByIdsAsync(
                queueType,
                new[] { jobInfos[0].Id, jobInfos[2].Id },
                false,
                CancellationToken.None)).ToList();

            Assert.Equal(2, retrievedJobInfos.Count);
            var ids = new List<long> { retrievedJobInfos[0].Id, retrievedJobInfos[1].Id };
            Assert.Contains(jobInfos[0].Id, ids);
            Assert.Contains(jobInfos[2].Id, ids);
        }

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

            var retrievedJobInfos =
                (await _azureStorageJobQueueClient.GetJobByGroupIdAsync(queueType, 2, true, CancellationToken.None))
                .ToList();

            Assert.Equal(2, retrievedJobInfos.Count);
            var definitions = new List<string>
                { retrievedJobInfos.First().Definition, retrievedJobInfos.Last().Definition };
            Assert.Contains("job4", definitions);
            Assert.Contains("job5", definitions);
        }

        [Fact]
        public async Task GivenJobsWithDifferentStatus_WhenCancelJobById_ThenTheStatusShouldBeSetCorrectly()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsWithDifferentStatus_WhenCancelJobById_ThenTheStatusShouldBeSetCorrectly;

            // enqueue jobs
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3", "job4" },
                1,
                false,
                false,
                CancellationToken.None)).ToList();

            var jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            var jobInfo2 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            var jobInfo3 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo2.Status = JobStatus.Failed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);

            jobInfo3.Status = JobStatus.Completed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo3, false, CancellationToken.None);

            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfos[0].Id, CancellationToken.None);
            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfos[1].Id, CancellationToken.None);
            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfos[2].Id, CancellationToken.None);
            await _azureStorageJobQueueClient.CancelJobByIdAsync(queueType, jobInfos[3].Id, CancellationToken.None);

            var retrievedJobInfos = (await _azureStorageJobQueueClient.GetJobsByIdsAsync(queueType, jobInfos.Select(j => j.Id).ToArray(), false, CancellationToken.None)).ToList();

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
        public async Task GivenJobsWithDifferentStatus_WhenCancelJobByGroupId_ThenTheStatusShouldBeSetCorrectly()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsWithDifferentStatus_WhenCancelJobByGroupId_ThenTheStatusShouldBeSetCorrectly;

            // enqueue jobs
            var jobInfos = (await _azureStorageJobQueueClient.EnqueueAsync(
                queueType,
                new[] { "job1", "job2", "job3", "job4" },
                1,
                false,
                false,
                CancellationToken.None)).ToList();

            var jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            var jobInfo2 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            var jobInfo3 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            jobInfo2.Status = JobStatus.Failed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);

            jobInfo3.Status = JobStatus.Completed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo3, false, CancellationToken.None);

            await _azureStorageJobQueueClient.CancelJobByGroupIdAsync(queueType, 1, CancellationToken.None);

            var retrievedJobInfos = (await _azureStorageJobQueueClient.GetJobsByIdsAsync(queueType, jobInfos.Select(j => j.Id).ToArray(), false, CancellationToken.None)).ToList();

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

            var jobInfo1 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);

            var jobInfoEntity = ((FhirToDataLakeAzureStorageJobInfo)jobInfo1).ToTableEntity();

            var jobLockEntity = (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(
                jobInfoEntity.PartitionKey,
                AzureStorageKeyProvider.JobLockRowKey(((FhirToDataLakeAzureStorageJobInfo)jobInfo1).JobIdentifier()),
                cancellationToken: CancellationToken.None)).Value;

            // the message is updated, while table entity isn't update
            await _azureJobMessageQueueClient.UpdateMessageAsync(
                jobLockEntity.GetString(JobLockEntityProperties.JobMessageId),
                jobLockEntity.GetString(JobLockEntityProperties.JobMessagePopReceipt),
                visibilityTimeout: TimeSpan.FromSeconds((long)jobInfoEntity[JobInfoEntityProperties.HeartbeatTimeoutSec]),
                cancellationToken: CancellationToken.None);

            // keep alive should throw exception
            var exception = await Assert.ThrowsAsync<RequestFailedException>(async () => await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
            Assert.Equal("PopReceiptMismatch", exception.ErrorCode);

            // the message is still invisible
            Assert.Null(await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None));

            await Task.Delay(TimeSpan.FromSeconds(HeartbeatTimeoutSec));

            // keep alive should still throw exception
            exception = await Assert.ThrowsAsync<RequestFailedException>(async () => await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
            Assert.Equal("PopReceiptMismatch", exception.ErrorCode);

            // re-dequeue
            var jobInfo2 = await _azureStorageJobQueueClient.DequeueAsync(queueType, TestWorkerName, HeartbeatTimeoutSec, CancellationToken.None);
            Assert.NotNull(jobInfo2);
            Assert.Equal(jobInfo1.Id, jobInfo2.Id);

            // keep alive should throw JobNotExistException for jobInfo1 as the version doesn't match
            await Assert.ThrowsAsync<JobNotExistException>(async () => await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));

            // keep alive successfully for jobInfo2
            var shouldCancel = await _azureStorageJobQueueClient.KeepAliveJobAsync(jobInfo2, CancellationToken.None);
            Assert.False(shouldCancel);

            // complete jobInfo1 should throw JobNotExistException
            jobInfo1.Status = JobStatus.Completed;
            await Assert.ThrowsAsync<JobNotExistException>(async () => await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo1, true, CancellationToken.None));

            // complete jobInfo2 successfully
            jobInfo2.Status = JobStatus.Completed;
            await _azureStorageJobQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);
        }

        private async Task CleanStorage()
        {
            await _azureJobMessageQueueClient.ClearMessagesAsync();
        }

        private async Task<Tuple<JobInfo, TableEntity>> EnqueueStepBySteps(long jobId, byte queueType, int steps)
        {
            var jobInfo = new FhirToDataLakeAzureStorageJobInfo()
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
                new (TableTransactionActionType.Add, jobInfoEntity),
                new (TableTransactionActionType.Add, jobLockEntity),
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
            var retrievedJobInfo =
                await _azureStorageJobQueueClient.GetJobByIdAsync(
                    jobInfo.QueueType,
                    jobInfo.Id,
                    true,
                    CancellationToken.None);
            Assert.Equal(jobInfo.Id, retrievedJobInfo.Id);
            Assert.Equal(jobInfo.Definition, retrievedJobInfo.Definition);

            // check table entity
            // job reverse index entity should exist
            var reversePartitionKey = AzureStorageKeyProvider.JobReverseIndexPartitionKey(jobInfo.QueueType, retrievedJobInfo.Id);
            var reverseRowKey = AzureStorageKeyProvider.JobReverseIndexRowKey(jobInfo.QueueType, retrievedJobInfo.Id);
            var reverseIndexEntity = (await _azureJobInfoTableClient.GetEntityAsync<JobReverseIndexEntity>(reversePartitionKey, reverseRowKey)).Value;
            Assert.NotNull(reverseIndexEntity);

            // job info entity should exist
            var retrievedJobInfoEntity = (await _azureJobInfoTableClient.GetEntityAsync<TableEntity>(reverseIndexEntity.JobInfoEntityPartitionKey, reverseIndexEntity.JobInfoEntityRowKey)).Value;
            Assert.NotNull(retrievedJobInfoEntity);

            // job lock entity should exist
            var jobLockEntityRowKey =
                AzureStorageKeyProvider.JobLockRowKey(((FhirToDataLakeAzureStorageJobInfo)retrievedJobInfo).JobIdentifier());
            var retrievedJobLockEntity =
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