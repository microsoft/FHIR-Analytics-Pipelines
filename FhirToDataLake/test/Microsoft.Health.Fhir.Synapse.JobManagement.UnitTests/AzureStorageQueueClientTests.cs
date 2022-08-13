// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Data.Tables;
using Azure.Storage.Queues;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
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
    }

    // TODO: the unit tests are copied from sql queue client, will add more unit tests for azure storage
    [Trait("Category", "JobManagementTests")]
    public class AzureStorageQueueClientTests
    {
        private readonly NullLogger<AzureStorageQueueClient<FhirToDataLakeAzureStorageJobInfo>>
            _nullAzureStorageQueueClientLogger =
                NullLogger<AzureStorageQueueClient<FhirToDataLakeAzureStorageJobInfo>>.Instance;

        private const string _testAgentName = "testAgentName";
        private readonly byte _queueType = (byte) QueueType.FhirToDataLake;

        private readonly AzureStorageQueueClient<FhirToDataLakeAzureStorageJobInfo> _azureStorageQueueClient;
        private readonly TableClient _azureJobInfoTableClient;
        private readonly QueueClient _azureJobMessageQueueClient;

        // TODO: move to a constant file and use it for both queue client and unit tests.
        private const int QueueMessageVisibilityTimeoutInSeconds = 60;

        public AzureStorageQueueClientTests()
        {
            var azuriteEmulatorStorage = new AzuriteEmulatorStorage(_testAgentName);
            var jobConfig = new JobConfiguration
            {
                QueueType = QueueType.FhirToDataLake,
            };

            _azureJobInfoTableClient = new TableClient(azuriteEmulatorStorage.TableUrl, azuriteEmulatorStorage.TableName);
            _azureJobMessageQueueClient = new QueueClient(azuriteEmulatorStorage.QueueUrl, azuriteEmulatorStorage.QueueName);

            // Delete table and queue if exists
            _azureJobInfoTableClient.Delete();
            _azureJobMessageQueueClient.DeleteIfExists();

            _azureStorageQueueClient = new AzureStorageQueueClient<FhirToDataLakeAzureStorageJobInfo>(
                azuriteEmulatorStorage,
                Options.Create<JobConfiguration>(jobConfig),
                _nullAzureStorageQueueClientLogger);

            _azureStorageQueueClient.IsInitialized();
        }

        [Fact]
        public async Task GivenNewJobs_WhenEnqueueJobs_ThenCreatedJobsShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenNewJobs_WhenEnqueueJobs_ThenCreatedJobsShouldBeReturned;

            var definitions = new[] { "job1", "job2" };
            var jobInfos = (await _azureStorageQueueClient.EnqueueAsync(
                queueType,
                definitions,
                null,
                false,
                false,
                CancellationToken.None)).ToList();

            // check job info
            Assert.Equal(2, jobInfos.Count());
            Assert.Equal(1, jobInfos.Last().Id - jobInfos.First().Id);
            Assert.Equal(JobStatus.Created, jobInfos.First().Status);
            Assert.Null(jobInfos.First().StartDate);
            Assert.Null(jobInfos.First().EndDate);
            Assert.Equal(jobInfos.Last().GroupId, jobInfos.First().GroupId);

            var jobInfo =
                await _azureStorageQueueClient.GetJobByIdAsync(
                    queueType,
                    jobInfos.First().Id,
                    true,
                    CancellationToken.None);
            Assert.Contains(jobInfo.Definition, definitions);

            jobInfo = await _azureStorageQueueClient.GetJobByIdAsync(
                queueType,
                jobInfos.Last().Id,
                true,
                CancellationToken.None);
            Assert.Contains(jobInfo.Definition, definitions);

            // TODO: check message queue

            // TODO: check table
        }

        [Fact]
        public async Task GivenNewJobsWithSameQueueType_WhenEnqueueWithForceOneActiveJobGroup_ThenSecondJobShouldNotBeEnqueued()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenNewJobsWithSameQueueType_WhenEnqueueWithForceOneActiveJobGroup_ThenSecondJobShouldNotBeEnqueued;

            // TODO: this field ForceOneActiveJobGroup isn't used need to add it?
            // var jobInfos = await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1" }, null, true, false, CancellationToken.None);
            // await Assert.ThrowsAsync<JobConflictException>(async () => await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job2" }, null, true, false, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobsWithSameDefinition_WhenEnqueue_ThenOnlyOneJobShouldBeEnqueued()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsWithSameDefinition_WhenEnqueue_ThenOnlyOneJobShouldBeEnqueued;

            var jobInfos = (await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1" }, null, false, false, CancellationToken.None)).ToList();
            Assert.Single(jobInfos);
            var jobId = jobInfos.First().Id;
            jobInfos = (await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1" }, null, false, false, CancellationToken.None)).ToList();
            Assert.Equal(jobId, jobInfos.First().Id);
        }

        [Fact]
        public async Task GivenJobsWithSameDefinition_WhenEnqueueWithGroupId_ThenGroupIdShouldBeCorrect()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsWithSameDefinition_WhenEnqueueWithGroupId_ThenGroupIdShouldBeCorrect;

            long groupId = new Random().Next(int.MinValue, int.MaxValue);
            var jobInfos = (await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1", "job2" }, groupId, false, false, CancellationToken.None)).ToList();
            Assert.Equal(2, jobInfos.Count());
            Assert.Equal(groupId, jobInfos.First().GroupId);
            Assert.Equal(groupId, jobInfos.Last().GroupId);
            jobInfos = (await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job3", "job4" }, groupId, false, false, CancellationToken.None)).ToList();
            Assert.Equal(2, jobInfos.Count());
            Assert.Equal(groupId, jobInfos.First().GroupId);
            Assert.Equal(groupId, jobInfos.Last().GroupId);
        }

        [Fact]
        public async Task GivenJobsEnqueue_WhenDequeue_ThenAllJobsShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobsEnqueue_WhenDequeue_ThenAllJobsShouldBeReturned;

            await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1" }, null, false, false, CancellationToken.None);
            await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job2" }, null, false, false, CancellationToken.None);

            var definitions = new List<string>();
            var jobInfo1 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 10, CancellationToken.None);
            definitions.Add(jobInfo1.Definition);
            var jobInfo2 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 10, CancellationToken.None);
            definitions.Add(jobInfo2.Definition);
            Assert.Null(await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 10, CancellationToken.None));

            Assert.Contains("job1", definitions);
            Assert.Contains("job2", definitions);
        }

        [Fact]
        public async Task GivenJobKeepPutHeartbeat_WhenDequeue_ThenJobShouldNotBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobKeepPutHeartbeat_WhenDequeue_ThenJobShouldNotBeReturned;

            var jobs = await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { $"job1-{queueType}" }, null, false, false, CancellationToken.None);
            Assert.Single(jobs);

            var jobInfo1 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 1, CancellationToken.None);
            jobInfo1.QueueType = queueType;
            Assert.Equal(jobInfo1.Id, jobs.First().Id);
            await Task.Delay(TimeSpan.FromSeconds(5));
            var cancelRequested = await _azureStorageQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None);
            Assert.False(cancelRequested);
            Assert.Null(await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 4, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobKeepPutHeartbeatWithResult_WhenDequeue_ThenJobWithResultShouldNotBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobKeepPutHeartbeatWithResult_WhenDequeue_ThenJobWithResultShouldNotBeReturned;

            await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1" }, null, false, false, CancellationToken.None);

            var jobInfo1 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 1, CancellationToken.None);
            jobInfo1.QueueType = queueType;
            jobInfo1.Result = "current-result";
            await _azureStorageQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None);
            await Task.Delay(TimeSpan.FromSeconds(QueueMessageVisibilityTimeoutInSeconds));
            var jobInfo2 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);
            Assert.Equal(jobInfo1.Result, jobInfo2.Result);
        }

        [Fact]
        public async Task GivenRunningJobCancelled_WhenKeepHeartbeat_ThenCancelRequestedShouldBeReturned()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenRunningJobCancelled_WhenKeepHeartbeat_ThenCancelRequestedShouldBeReturned;

            await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1" }, null, false, false, CancellationToken.None);

            var jobInfo1 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 10, CancellationToken.None);
            jobInfo1.QueueType = queueType;
            jobInfo1.Result = "current-result";
            Assert.False(await _azureStorageQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
            await _azureStorageQueueClient.CancelJobByGroupIdAsync(queueType, jobInfo1.GroupId, CancellationToken.None);
            Assert.True(await _azureStorageQueueClient.KeepAliveJobAsync(jobInfo1, CancellationToken.None));
        }

        [Fact]
        public async Task GivenJobNotHeartbeat_WhenDequeue_ThenJobShouldBeReturnedAgain()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenJobNotHeartbeat_WhenDequeue_ThenJobShouldBeReturnedAgain;

            await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1" }, null, false, false, CancellationToken.None);

            var jobInfo1 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);

            // TODO: heartbeatTimeoutSec used for queue visible timeout?
            await Task.Delay(TimeSpan.FromSeconds(QueueMessageVisibilityTimeoutInSeconds));
            var jobInfo2 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);
            Assert.Null(await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 10, CancellationToken.None));

            Assert.Equal(jobInfo1.Id, jobInfo2.Id);
            Assert.True(jobInfo1.Version < jobInfo2.Version);
        }

        [Fact]
        public async Task GivenGroupJobs_WhenCompleteJob_ThenJobsShouldBeCompleted()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenGroupJobs_WhenCompleteJob_ThenJobsShouldBeCompleted;

            await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1", "job2" }, null, false, false, CancellationToken.None);

            var jobInfo1 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);
            var jobInfo2 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 10, CancellationToken.None);

            Assert.Equal(JobStatus.Running, jobInfo1.Status);
            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for cancellation";
            await _azureStorageQueueClient.CompleteJobAsync(jobInfo1, false, CancellationToken.None);
            var jobInfo = await _azureStorageQueueClient.GetJobByIdAsync(queueType, jobInfo1.Id, false, CancellationToken.None);
            Assert.Equal(JobStatus.Failed, jobInfo.Status);
            Assert.Equal(jobInfo1.Result, jobInfo.Result);

            jobInfo2.Status = JobStatus.Completed;
            jobInfo2.Result = "Completed";
            await _azureStorageQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);
            jobInfo = await _azureStorageQueueClient.GetJobByIdAsync(queueType, jobInfo2.Id, false, CancellationToken.None);
            Assert.Equal(JobStatus.Completed, jobInfo.Status);
            Assert.Equal(jobInfo2.Result, jobInfo.Result);
        }

        [Fact]
        public async Task GivenGroupJobs_WhenCancelJobsByGroupId_ThenAllJobsShouldBeCancelled()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenGroupJobs_WhenCancelJobsByGroupId_ThenAllJobsShouldBeCancelled;

            await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1", "job2", "job3" }, null, false, false, CancellationToken.None);

            var jobInfo1 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);
            var jobInfo2 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);

            await _azureStorageQueueClient.CancelJobByGroupIdAsync(queueType, jobInfo1.GroupId, CancellationToken.None);
            Assert.True((await _azureStorageQueueClient.GetJobByGroupIdAsync(queueType, jobInfo1.GroupId, false, CancellationToken.None)).All(t => t.Status == JobStatus.Cancelled || (t.Status == JobStatus.Running && t.CancelRequested)));

            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for cancellation";
            await _azureStorageQueueClient.CompleteJobAsync(jobInfo1, false, CancellationToken.None);
            var jobInfo = await _azureStorageQueueClient.GetJobByIdAsync(queueType, jobInfo1.Id, false, CancellationToken.None);
            Assert.Equal(JobStatus.Failed, jobInfo.Status);
            Assert.Equal(jobInfo1.Result, jobInfo.Result);

            jobInfo2.Status = JobStatus.Completed;
            jobInfo2.Result = "Completed";
            await _azureStorageQueueClient.CompleteJobAsync(jobInfo2, false, CancellationToken.None);
            jobInfo = await _azureStorageQueueClient.GetJobByIdAsync(queueType, jobInfo2.Id, false, CancellationToken.None);
            Assert.Equal(JobStatus.Cancelled, jobInfo.Status);
            Assert.Equal(jobInfo2.Result, jobInfo.Result);
        }

        [Fact]
        public async Task GivenGroupJobs_WhenCancelJobsById_ThenOnlySingleJobShouldBeCancelled()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenGroupJobs_WhenCancelJobsById_ThenOnlySingleJobShouldBeCancelled;

            var jobs = await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1", "job2", "job3" }, null, false, false, CancellationToken.None);

            await _azureStorageQueueClient.CancelJobByIdAsync(queueType, jobs.First().Id, CancellationToken.None);
            Assert.Equal(JobStatus.Cancelled, (await _azureStorageQueueClient.GetJobByIdAsync(queueType, jobs.First().Id, false, CancellationToken.None)).Status);

            // TODO: job1 is cancelled, dequeue job2?
            var jobInfo1 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);
            Assert.Null(jobInfo1);

            var jobInfo2 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);
            var jobInfo3 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);

            Assert.False(jobInfo2.CancelRequested);
            Assert.False(jobInfo3.CancelRequested);
        }

        [Fact]
        public async Task GivenGroupJobs_WhenOneJobFailedAndRequestCancellation_ThenAllJobsShouldBeCancelled()
        {
            await CleanStorage();

            const byte queueType = (byte)TestQueueType.GivenGroupJobs_WhenOneJobFailedAndRequestCancellation_ThenAllJobsShouldBeCancelled;

            await _azureStorageQueueClient.EnqueueAsync(queueType, new string[] { "job1", "job2", "job3" }, null, false, false, CancellationToken.None);

            var jobInfo1 = await _azureStorageQueueClient.DequeueAsync(queueType, "test-worker", 0, CancellationToken.None);
            jobInfo1.Status = JobStatus.Failed;
            jobInfo1.Result = "Failed for critical error";

            await _azureStorageQueueClient.CompleteJobAsync(jobInfo1, true, CancellationToken.None);
            Assert.True((await _azureStorageQueueClient.GetJobByGroupIdAsync(queueType, jobInfo1.GroupId, false, CancellationToken.None)).All(t => t.Status is (JobStatus?)JobStatus.Cancelled or (JobStatus?)JobStatus.Failed));
        }

        private async Task CleanStorage()
        {
            await _azureJobMessageQueueClient.ClearMessagesAsync();
        }

    }
}