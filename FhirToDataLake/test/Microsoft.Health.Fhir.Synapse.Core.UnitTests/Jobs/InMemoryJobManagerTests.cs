// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    [Trait("Category", "Job")]
    public class InMemoryJobManagerTests
    {
        private const string TestContainerName = "TestJobContainer";
        private static readonly DateTimeOffset _testStartTime = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.FromHours(0));
        private static readonly DateTimeOffset _testEndTime = new DateTimeOffset(2020, 11, 1, 0, 0, 0, TimeSpan.FromHours(0));
        private static readonly List<string> _testResourceTypeFilters = new List<string> { "Patient", "Observation" };

        [Fact]
        public async Task GivenABrokenJobStore_WhenExecute_ExceptionShouldBeThrown()
        {
            var jobManager = CreateJobManager(CreateBrokenJobStore(), _testStartTime, _testEndTime, _testResourceTypeFilters);
            await Assert.ThrowsAsync<Exception>(() => jobManager.RunAsync());
        }

        [Fact]
        public async Task GivenALeasedActiveJobRunning_WhenExecute_NoJobWillBeCreated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
            };
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);

            // Create an active job.
            var activeJob = new Job(
                TestContainerName,
                JobStatus.Running,
                _testResourceTypeFilters,
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));
            await blobClient.CreateJob(activeJob, "SampleLease");

            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime, _testResourceTypeFilters);

            await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.RunAsync());

            // Only one job is running.
            var activeJobs = await blobClient.ListBlobsAsync(AzureBlobJobConstants.ActiveJobFolder);
            Assert.Single(activeJobs);

            jobManager.Dispose();
        }

        [Fact]
        public async Task GivenJobLockReleased_WhenExecute_JobWillBeResumed()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
            };
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);

            var activeJob = new Job(
                TestContainerName,
                JobStatus.Running,
                _testResourceTypeFilters,
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));
            await blobClient.CreateJob(activeJob, "SampleLease");

            await blobClient.ReleaseLeaseAsync(AzureBlobJobConstants.JobLockFileName, "SampleLease");

            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime, _testResourceTypeFilters);
            await jobManager.RunAsync();

            // The running job has been resumed and completed.
            var job = await blobClient.GetValue<Job>($"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            Assert.Null(job);
            var completedJob = await blobClient.GetValue<Job>($"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.NotNull(completedJob);

            jobManager.Dispose();
        }

        [Fact]
        public async Task GivenNoActiveJob_WhenExecute_NewJobWillBeTriggered()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
            };
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);
            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime, _testResourceTypeFilters);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            await jobManager.RunAsync();
            Assert.NotEmpty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var blobName = (await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder)).First();
            var blob = await blobClient.GetBlobAsync(blobName);
            using var streamReader = new StreamReader(blob);
            var job = JsonConvert.DeserializeObject<Job>(streamReader.ReadToEnd());

            Assert.Equal(JobStatus.Succeeded, job.Status);
            Assert.Equal(_testStartTime, job.DataPeriod.Start);

            // Test end data period
            Assert.Equal(_testEndTime, job.DataPeriod.End);

            jobManager.Dispose();
        }

        [Fact]
        public async Task GivenNoActiveJobAndAJobUnfinished_WhenExecute_NewJobWillBeTriggered()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
            };

            var failedJob = new Job(
                TestContainerName,
                JobStatus.Failed,
                _testResourceTypeFilters,
                new DataPeriod(_testStartTime, _testEndTime),
                DateTimeOffset.Now.AddMinutes(-1));

            var metadata = new SchedulerMetadata();
            metadata.UnfinishedJobs = new List<Job>()
            {
                failedJob,
            };

            await blobClient.CreateBlobAsync(
                AzureBlobJobConstants.SchedulerMetadataFileName,
                new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(metadata))));
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);
            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime, _testResourceTypeFilters);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            await jobManager.RunAsync();
            Assert.NotEmpty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var blobName = (await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder)).First();
            var blob = await blobClient.GetBlobAsync(blobName);
            using var streamReader = new StreamReader(blob);
            var job = JsonConvert.DeserializeObject<Job>(streamReader.ReadToEnd());

            Assert.Equal(JobStatus.Succeeded, job.Status);
            Assert.Equal(failedJob.Id, job.ResumedJobId);
            Assert.Equal(_testStartTime, job.DataPeriod.Start);
            Assert.Equal(_testEndTime, job.DataPeriod.End);

            using var streamReader2 = new StreamReader(await blobClient.GetBlobAsync($"{AzureBlobJobConstants.SchedulerMetadataFileName}"));
            var newMetadata = JsonConvert.DeserializeObject<SchedulerMetadata>(streamReader2.ReadToEnd());
            Assert.Empty(newMetadata.UnfinishedJobs);

            jobManager.Dispose();
        }

        [Fact]
        public async Task GivenJobEndTimeInFuture_WhenCreateNewJob_NewJobEndTimeShouldBeCorrect()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
            };
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);
            var jobManager = CreateJobManager(jobStore, _testStartTime, DateTimeOffset.MaxValue, _testResourceTypeFilters);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            await jobManager.RunAsync();
            Assert.NotEmpty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var blobName = (await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder)).First();
            var blob = await blobClient.GetBlobAsync(blobName);
            using var streamReader = new StreamReader(blob);
            var job = JsonConvert.DeserializeObject<Job>(streamReader.ReadToEnd());

            Assert.Equal(JobStatus.Succeeded, job.Status);
            Assert.Equal(_testStartTime, job.DataPeriod.Start);

            // Test end data period
            Assert.True(job.DataPeriod.End < DateTimeOffset.UtcNow.AddMinutes(-2));
            Assert.True(job.DataPeriod.End > DateTimeOffset.UtcNow.AddMinutes(-3));

            jobManager.Dispose();
        }

        [Fact]
        public async Task GivenInvalidTimeRange_WhenExecute_ExceptionWillBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient, new JobConfiguration
            {
                ContainerName = TestContainerName,
            });
            var jobManager = CreateJobManager(jobStore, DateTimeOffset.UtcNow.AddDays(1), DateTimeOffset.MaxValue, _testResourceTypeFilters);

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.RunAsync());
            Assert.Contains("trigger is in the future", exception.Message);

            jobManager.Dispose();
        }

        [Fact]
        public async Task GivenCompletedTimeRange_WhenExecute_ExceptionWillBeThrown()
        {
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
            };

            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);
            var jobManager = CreateJobManager(jobStore, _testEndTime, _testEndTime, _testResourceTypeFilters);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.RunAsync());
            Assert.Contains("Job has been scheduled to end", exception.Message);

            jobManager.Dispose();
        }

        private static IJobStore CreateBrokenJobStore()
        {
            var jobStore = Substitute.For<IJobStore>();
            jobStore.AcquireActiveJobAsync().Returns(Task.FromException<Job>(new Exception()));
            return jobStore;
        }

        private static AzureBlobJobStore CreateInMemoryJobStore(IAzureBlobContainerClient blobClient, JobConfiguration config = null)
        {
            var storeConfiguration = new DataLakeStoreConfiguration
            {
                StorageUrl = "http://test.blob.core.windows.net",
            };
            var mockFactory = Substitute.For<IAzureBlobContainerClientFactory>();
            mockFactory.Create(Arg.Any<string>(), Arg.Any<string>()).ReturnsForAnyArgs(blobClient);
            return new AzureBlobJobStore(
                mockFactory,
                Options.Create(config),
                Options.Create(storeConfiguration),
                new NullLogger<AzureBlobJobStore>());
        }

        private static TaskResult CreateTestTaskResult()
        {
            return new TaskResult("Patient", null, 0, 100, 0, 100, string.Empty);
        }

        private JobManager CreateJobManager(
            IJobStore jobStore,
            DateTimeOffset start,
            DateTimeOffset end,
            IEnumerable<string> resourceTypeFilters)
        {
            var schedulerConfig = new JobSchedulerConfiguration
            {
                MaxConcurrencyCount = 10,
            };

            var jobConfiguration = new JobConfiguration
            {
                ContainerName = TestContainerName,
                StartTime = start,
                EndTime = end,
                ResourceTypeFilters = resourceTypeFilters,
            };

            var taskExecutor = Substitute.For<ITaskExecutor>();
            taskExecutor.ExecuteAsync(Arg.Any<TaskContext>(), Arg.Any<JobProgressUpdater>(), Arg.Any<CancellationToken>()).Returns(CreateTestTaskResult());
            var jobExecutor = new JobExecutor(taskExecutor, new JobProgressUpdaterFactory(jobStore, new NullLoggerFactory()), Options.Create(schedulerConfig), new NullLogger<JobExecutor>());

            return new JobManager(
                jobStore,
                jobExecutor,
                new R4FhirSpecificationProvider(),
                Options.Create<JobConfiguration>(jobConfiguration),
                new NullLogger<JobManager>());
        }
    }
}
