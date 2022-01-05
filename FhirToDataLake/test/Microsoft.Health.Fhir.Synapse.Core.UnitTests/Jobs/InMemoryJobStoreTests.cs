// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Azure.Blob;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    [Trait("Category", "Scheduler")]

    public class InMemoryJobStoreTests
    {
        private const string ContainerName = "TestJobContainer";

        [Fact]
        public async Task GivenEmptyJobStore_WhenAcquireLock_OperationShouldSucceed()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            Assert.True(await jobStore.AcquireJobLock());
        }

        [Fact]
        public async Task GivenLockedJobStore_WhenAcquireLock_OperationShouldFail()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            Assert.True(await jobStore.AcquireJobLock());

            var jobStore2 = CreateInMemoryJobStore(blobClient);
            Assert.False(await jobStore2.AcquireJobLock());
        }

        [Fact]
        public async Task GivenReleasedJobStore_WhenAcquireLock_OperationShouldSucceed()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            Assert.True(await jobStore.AcquireJobLock());

            Assert.True(await jobStore.ReleaseJobLock());

            var jobStore2 = CreateInMemoryJobStore(blobClient);
            Assert.True(await jobStore2.AcquireJobLock());
        }

        [Fact]
        public async Task GivenReleasedJobStore_WhenReleaseLock_OperationShouldSucceed()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            Assert.True(await jobStore.AcquireJobLock());

            Assert.True(await jobStore.ReleaseJobLock());

            Assert.True(await jobStore.ReleaseJobLock());
        }

        [Fact]
        public async Task GivenASchedulerSettingObject_WhenGet_CorrectDataShouldBeReturned()
        {
            var testTime = new DateTimeOffset(new DateTime(2020, 01, 01));
            var schedulerSetting = new SchedulerMetadata { LastScheduledTimestamp = testTime };
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var emptyResult = await jobStore.GetSchedulerMetadata();
            Assert.Null(emptyResult);

            await blobClient.CreateBlobAsync(
                AzureBlobJobConstants.SchedulerMetadataFileName,
                new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(schedulerSetting))));
            var result = await jobStore.GetSchedulerMetadata();
            Assert.Equal(result.LastScheduledTimestamp, testTime);
        }

        [Fact]
        public async Task GivenANonCompletedJob_WhenCompleteJob_ExceptionShouldBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var activeJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-1));

            var jobStore = CreateInMemoryJobStore(blobClient);
            await Assert.ThrowsAsync<ArgumentException>(() => jobStore.CompleteJobAsync(activeJob, default));
        }

        [Fact]
        public async Task GivenACompletedJob_WhenCompleteJob_JobShouldBeUpdated()
        {
            var testTime = new DateTimeOffset(new DateTime(2020, 01, 01));

            var blobClient = new InMemoryBlobContainerClient();
            var activeJob = new Job(
                ContainerName,
                JobStatus.Completed,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, testTime),
                DateTimeOffset.Now.AddMinutes(-1));

            var jobStore = CreateInMemoryJobStore(blobClient);

            var schedulerSetting = await jobStore.GetSchedulerMetadata();
            Assert.Null(schedulerSetting);

            await jobStore.CompleteJobAsync(activeJob, default);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.ActiveJobFolder));
            Assert.NotEmpty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));
            schedulerSetting = await jobStore.GetSchedulerMetadata();
            Assert.Equal(schedulerSetting.LastScheduledTimestamp, testTime);
        }

        [Fact]
        public async Task GivenAFailedJob_WhenCompleteJob_ExceptionShouldBeThrown()
        {
            var testTime = new DateTimeOffset(new DateTime(2021, 02, 02));

            var blobClient = new InMemoryBlobContainerClient();
            var activeJob = new Job(
                ContainerName,
                JobStatus.Failed,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, testTime),
                DateTimeOffset.Now.AddMinutes(-1));

            var jobStore = CreateInMemoryJobStore(blobClient);

            var schedulerSetting = await jobStore.GetSchedulerMetadata();
            Assert.Null(schedulerSetting);

            await Assert.ThrowsAsync<ArgumentException>(() => jobStore.CompleteJobAsync(activeJob, default));
        }

        [Fact]
        public async Task GivenANewJob_WhenUpdateJob_JobShouldBeUpdated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var activeJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-1));

            var jobStore = CreateInMemoryJobStore(blobClient);
            await jobStore.UpdateJobAsync(activeJob, default);

            var savedJob = (await jobStore.GetActiveJobsAsync()).First();
            Assert.Equal(activeJob.Id, savedJob.Id);
        }

        [Fact]
        public async Task GivenAnExistingJob_WhenUpdateJob_JobShouldBeUpdated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var activeJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-1));

            var jobStore = CreateInMemoryJobStore(blobClient);
            await jobStore.UpdateJobAsync(activeJob, default);
            var savedJob1 = (await jobStore.GetActiveJobsAsync()).First();

            activeJob.ProcessedResourceCounts["Patient"] = 1000;
            activeJob.ResourceProgresses["Patient"] = "db=92asd";
            await jobStore.UpdateJobAsync(activeJob, default);
            var savedJob2 = (await jobStore.GetActiveJobsAsync()).First();

            Assert.Equal(savedJob1.Id, savedJob2.Id);
            Assert.Equal("db=92asd", savedJob2.ResourceProgresses["Patient"]);
            Assert.Equal(1000, savedJob2.ProcessedResourceCounts["Patient"]);
            Assert.NotEqual(savedJob1.LastHeartBeat, savedJob2.LastHeartBeat);
        }

        [Fact]
        public async Task GivenActiveJobRunning_WhenGetActiveJobs_CorrectResultShouldBeReturned()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var jobs = await jobStore.GetActiveJobsAsync();
            Assert.Empty(jobs);

            var activeJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(activeJob));
            await blobClient.CreateBlobAsync($"{AzureBlobJobConstants.ActiveJobFolder}/1.json", new MemoryStream(data));
            await blobClient.CreateBlobAsync($"{AzureBlobJobConstants.ActiveJobFolder}/2.json", new MemoryStream(data));
            await blobClient.CreateBlobAsync($"{AzureBlobJobConstants.ActiveJobFolder}/3.json", new MemoryStream(data));
            await blobClient.CreateBlobAsync($"{AzureBlobJobConstants.ActiveJobFolder}/4.json", new MemoryStream(data));
            await blobClient.CreateBlobAsync($"{AzureBlobJobConstants.ActiveJobFolder}/5.json", new MemoryStream(data));
            await blobClient.CreateBlobAsync($"{AzureBlobJobConstants.CompletedJobFolder}/1.json", new MemoryStream(data));
            await blobClient.CreateBlobAsync($"{AzureBlobJobConstants.FailedJobFolder}/1.json", new MemoryStream(data));

            jobs = await jobStore.GetActiveJobsAsync();
            Assert.Equal(5, jobs.Count());
        }

        private AzureBlobJobStore CreateInMemoryJobStore(InMemoryBlobContainerClient blobClient)
        {
            var jobConfiguration = new JobConfiguration
            {
                ContainerName = ContainerName,
                StartTime = DateTimeOffset.MinValue,
                EndTime = DateTimeOffset.MaxValue,
                ResourceTypeFilters = new List<string> { "Patient", "Observation" },
            };

            var storeConfiguration = new DataLakeStoreConfiguration
            {
                StorageUrl = "http://test.blob.core.windows.net",
            };
            var mockFactory = Substitute.For<IAzureBlobContainerClientFactory>();
            mockFactory.Create(Arg.Any<string>(), Arg.Any<string>()).ReturnsForAnyArgs(blobClient);
            return new AzureBlobJobStore(
                mockFactory,
                Options.Create(jobConfiguration),
                Options.Create(storeConfiguration),
                new NullLogger<AzureBlobJobStore>());
        }
    }
}
