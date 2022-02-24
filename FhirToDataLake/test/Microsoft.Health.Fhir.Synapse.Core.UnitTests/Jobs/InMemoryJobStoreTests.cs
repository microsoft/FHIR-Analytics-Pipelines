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
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    [Trait("Category", "Job")]

    public class InMemoryJobStoreTests
    {
        private const string TestContainerName = "TestJobContainer";
        private static readonly DateTimeOffset _testStartTime = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.FromHours(0));
        private static readonly DateTimeOffset _testEndTime = new DateTimeOffset(2020, 11, 1, 0, 0, 0, TimeSpan.FromHours(0));
        private static readonly List<string> _testResourceTypeFilters = new List<string> { "Patient", "Observation" };

        [Fact]
        public async Task GivenJobLocked_WhenAcquireJob_ExceptionWillBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            _ = await blobClient.AcquireLeaseAsync(AzureBlobJobConstants.JobLockFileName, null, TimeSpan.FromMinutes(1));

            var jobStore = CreateInMemoryJobStore(blobClient);
            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() => jobStore.AcquireActiveJobAsync());
            Assert.StartsWith("Another job is already started", exception.Message);

            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenNoActiveJobRunning_WhenAcquireJob_NoJobShouldBeReturned()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var job = await jobStore.AcquireActiveJobAsync();

            Assert.Null(job);
        }

        [Fact]
        public async Task GivenACompletedJobInActiveFolder_WhenAcquireJob_TheJobShouldBeCompleted_NoJobWillBeReturned()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var activeJob = new Job(
                TestContainerName,
                JobStatus.Succeeded,
                _testResourceTypeFilters,
                new DataPeriod(_testStartTime, _testEndTime),
                DateTimeOffset.Now.AddMinutes(-1));
            await blobClient.CreateJob(activeJob);

            var newJob = await jobStore.AcquireActiveJobAsync();

            // Assert new job is null.
            Assert.Null(newJob);

            // Assert job has been completed.
            var completedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.Equal(activeJob.Id, completedJob.Id);
            Assert.Equal(activeJob.DataPeriod.Start, completedJob.DataPeriod.Start);
            Assert.Equal(activeJob.DataPeriod.End, completedJob.DataPeriod.End);
            Assert.Equal(activeJob.ResourceTypes, completedJob.ResourceTypes);

            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenAFailedJobInActiveFolder_WhenAcquireJob_TheJobShouldBeCompletedAndNoJobWillBeReturned()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var activeJob = new Job(
                TestContainerName,
                JobStatus.Failed,
                _testResourceTypeFilters,
                new DataPeriod(_testStartTime, _testEndTime),
                DateTimeOffset.Now.AddMinutes(-1));
            await blobClient.CreateJob(activeJob);

            var newjob = await jobStore.AcquireActiveJobAsync();

            // Assert returned job is null.
            Assert.Null(newjob);

            // Assert the failed job has been completed.
            var savedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.Equal(activeJob.Id, savedJob.Id);
            Assert.Equal(JobStatus.Failed, savedJob.Status);
        }

        [Fact]
        public async Task GivenAnActiveJob_WhenUpdateJob_JobShouldBeUpdated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var activeJob = new Job(
                TestContainerName,
                JobStatus.Running,
                _testResourceTypeFilters,
                new DataPeriod(_testStartTime, _testEndTime),
                DateTimeOffset.Now.AddMinutes(-1));
            await blobClient.CreateJob(activeJob);

            activeJob.ResourceProgresses["Patient"] = "test1234";
            activeJob.PartIds["Patient"] = 2;
            activeJob.ProcessedResourceCounts["Patient"] = 100;
            activeJob.SkippedResourceCounts["Patient"] = 0;

            await jobStore.UpdateJobAsync(activeJob);

            var updatedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");

            // Assert job is updated.
            Assert.Equal(activeJob.Id, updatedJob.Id);
            Assert.Equal(JobStatus.Running, updatedJob.Status);
            Assert.Equal("test1234", activeJob.ResourceProgresses["Patient"]);
            Assert.Equal(2, activeJob.PartIds["Patient"]);
            Assert.Equal(100, activeJob.ProcessedResourceCounts["Patient"]);
            Assert.Equal(0, activeJob.SkippedResourceCounts["Patient"]);

            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenAnActiveJob_WhenCompleteJob_ArgumentExceptionShouldBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var activeJob = new Job(
                TestContainerName,
                JobStatus.Running,
                _testResourceTypeFilters,
                new DataPeriod(_testStartTime, _testEndTime),
                DateTimeOffset.Now.AddMinutes(-1));
            await blobClient.CreateJob(activeJob);

            await Assert.ThrowsAsync<ArgumentException>(() => jobStore.CompleteJobAsync(activeJob));
            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenASucceededJob_WhenCompleteJob_CompletedJobShouldPresent()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var activeJob = new Job(
                TestContainerName,
                JobStatus.Running,
                _testResourceTypeFilters,
                new DataPeriod(_testStartTime, _testEndTime),
                DateTimeOffset.Now.AddMinutes(-1));
            await blobClient.CreateJob(activeJob);

            // job has been persisted to active folder.
            var persistedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            Assert.NotNull(persistedJob);

            activeJob.Status = JobStatus.Succeeded;
            await jobStore.CompleteJobAsync(activeJob);

            // job has been moved to completed folder.
            Assert.Null(await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json"));
            var completedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.Equal(activeJob.Id, completedJob.Id);
        }

        [Fact]
        public async Task GivenAFailedJob_WhenCompleteJob_CompletedJobShouldPresent()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var activeJob = new Job(
                TestContainerName,
                JobStatus.Running,
                _testResourceTypeFilters,
                new DataPeriod(_testStartTime, _testEndTime),
                DateTimeOffset.Now.AddMinutes(-1));
            await blobClient.CreateJob(activeJob);

            // job has been persisted to active folder.
            var persistedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            Assert.NotNull(persistedJob);

            activeJob.Status = JobStatus.Failed;
            await jobStore.CompleteJobAsync(activeJob);

            // job has been moved to completed folder.
            Assert.Null(await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json"));
            var completedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.Equal(activeJob.Id, completedJob.Id);

            // job has been added to unfinishedJobs
            var stream = await blobClient.GetBlobAsync($"{AzureBlobJobConstants.SchedulerMetadataFileName}");
            using var streamReader = new StreamReader(stream);
            var metadata = JsonConvert.DeserializeObject<SchedulerMetadata>(streamReader.ReadToEnd());
            Assert.Equal(activeJob.Id, metadata.UnfinishedJobs.First().Id);
            Assert.Equal(activeJob.DataPeriod.Start, metadata.UnfinishedJobs.First().DataPeriod.Start);
            Assert.Equal(activeJob.DataPeriod.End, metadata.UnfinishedJobs.First().DataPeriod.End);
        }

        [Fact]
        public async Task GivenJobDataStaged_WhenCommit_CorrectResultShouldBeCommitted()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var activeJob = new Job(
                TestContainerName,
                JobStatus.Succeeded,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-1));

            // Upload parquet data
            var jobStore = CreateInMemoryJobStore(blobClient);
            var data = Encoding.UTF8.GetBytes("test");
            var stageBlobList = new List<string>
            {
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/01/Patient_{activeJob.Id}_00000.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/02/Patient_{activeJob.Id}_00001.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/03/Patient_{activeJob.Id}_00002.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/04/Patient_{activeJob.Id}_00003.parquet",
            };
            var resultBlobList = new List<string>
            {
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/01/{activeJob.Id}/Patient_{activeJob.Id}_00000.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/02/{activeJob.Id}/Patient_{activeJob.Id}_00001.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/03/{activeJob.Id}/Patient_{activeJob.Id}_00002.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/04/{activeJob.Id}/Patient_{activeJob.Id}_00003.parquet",
            };

            foreach (var blobName in stageBlobList)
            {
                await blobClient.CreateBlobAsync(blobName, new MemoryStream(data));
            }

            activeJob.PartIds["Patient"] = 4;
            await jobStore.CompleteJobAsync(activeJob);

            // Make sure data has been moved to result folder.
            foreach (var blobName in resultBlobList)
            {
                Assert.NotNull(await blobClient.GetBlobAsync(blobName));
            }

            foreach (var blobName in stageBlobList)
            {
                Assert.Null(await blobClient.GetBlobAsync(blobName));
            }

            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenJobDataStaged_WhenCommit_UnrecordedDataShouldBeRemoved()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var activeJob = new Job(
                TestContainerName,
                JobStatus.Succeeded,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-1));

            // Upload parquet data
            var jobStore = CreateInMemoryJobStore(blobClient);
            var data = Encoding.UTF8.GetBytes("test");
            var stageBlobList = new List<string>
            {
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/01/Patient_{activeJob.Id}_00000.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/02/Patient_{activeJob.Id}_00001.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/03/Patient_{activeJob.Id}_00002.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/04/Patient_{activeJob.Id}_00003.parquet",
            };
            var resultBlobList = new List<string>
            {
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/01/{activeJob.Id}/Patient_{activeJob.Id}_00000.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/02/{activeJob.Id}/Patient_{activeJob.Id}_00001.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/03/{activeJob.Id}/Patient_{activeJob.Id}_00002.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/04/{activeJob.Id}/Patient_{activeJob.Id}_00003.parquet",
            };

            foreach (var blobName in stageBlobList)
            {
                await blobClient.CreateBlobAsync(blobName, new MemoryStream(data));
            }

            // Only blobs of partId 0 and 1 should be kept.
            activeJob.PartIds["Patient"] = 2;
            await jobStore.CompleteJobAsync(activeJob);

            // Make sure data has been moved to result folder.
            Assert.NotNull(await blobClient.GetBlobAsync(resultBlobList[0]));
            Assert.NotNull(await blobClient.GetBlobAsync(resultBlobList[1]));
            Assert.Null(await blobClient.GetBlobAsync(resultBlobList[2]));
            Assert.Null(await blobClient.GetBlobAsync(resultBlobList[3]));

            foreach (var blobName in stageBlobList)
            {
                Assert.Null(await blobClient.GetBlobAsync(blobName));
            }

            jobStore.Dispose();
        }

        private static async Task<Job> LoadJobFromBlob(InMemoryBlobContainerClient blobClient, string blobName)
        {
            var stream = await blobClient.GetBlobAsync(blobName);
            if (stream == null)
            {
                return null;
            }

            using var streamReader = new StreamReader(stream);
            var text = streamReader.ReadToEnd();
            return JsonConvert.DeserializeObject<Job>(text);
        }

        private AzureBlobJobStore CreateInMemoryJobStore(
            InMemoryBlobContainerClient blobClient)
        {
            var jobConfiguration = new JobConfiguration
            {
                ContainerName = TestContainerName,
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
