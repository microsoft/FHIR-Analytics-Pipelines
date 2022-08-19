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
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
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
        private static readonly DateTimeOffset _testStartTime = new (2020, 1, 1, 0, 0, 0, TimeSpan.FromHours(0));
        private static readonly DateTimeOffset _testEndTime = new (2020, 11, 1, 0, 0, 0, TimeSpan.FromHours(0));

        private static readonly List<TypeFilter> _testResourceTypeFilters =
            new List<TypeFilter> { new ("Patient", null), new ("Observation", null) };

        private static readonly FilterInfo _filterInfo = new FilterInfo(FilterScope.System, null, _testStartTime, _testResourceTypeFilters, null);

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
        public async Task GivenAnActiveJobInActiveFolder_WhenAcquireJob_TheJobShouldBeReturned()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Running,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);
            await blobClient.CreateJob(activeJob);

            var returnedJob = await jobStore.AcquireActiveJobAsync();

            // Assert the returned job is not null.
            Assert.NotNull(returnedJob);

            Assert.Equal(activeJob.ToString(), returnedJob.ToString());

            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenASucceededJobInActiveFolder_WhenAcquireJob_TheJobShouldBeCompleted_NoJobWillBeReturned()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Succeeded,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);
            await blobClient.CreateJob(activeJob);

            var newJob = await jobStore.AcquireActiveJobAsync();

            // Assert new job is null.
            Assert.Null(newJob);

            // Assert job has been completed.
            var completedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.Equal(activeJob.Id, completedJob.Id);
            Assert.Equal(activeJob.DataPeriod.Start, completedJob.DataPeriod.Start);
            Assert.Equal(activeJob.DataPeriod.End, completedJob.DataPeriod.End);
            Assert.Equal(JobStatus.Succeeded, completedJob.Status);
            Assert.NotNull(completedJob.CompletedTime);

            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenAFailedJobInActiveFolder_WhenAcquireJob_TheJobShouldBeCompletedAndNoJobWillBeReturned()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Failed,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);
            await blobClient.CreateJob(activeJob);

            var newJob = await jobStore.AcquireActiveJobAsync();

            // Assert returned job is null.
            Assert.Null(newJob);

            // Assert the failed job has been completed.
            var savedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.Equal(activeJob.Id, savedJob.Id);
            Assert.Equal(JobStatus.Failed, savedJob.Status);
            Assert.NotNull(savedJob.CompletedTime);

            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenNullJob_WhenUpdateJob_ExceptionShouldBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            Job job = null;
            _ = await Assert.ThrowsAsync<ArgumentNullException>(() => jobStore.UpdateJobAsync(job));
        }

        [Fact]
        public async Task GivenNewJob_WhenUpdateJob_JobShouldBeUploaded()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var newJob = Job.Create(
                TestContainerName,
                JobStatus.New,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);

            await jobStore.UpdateJobAsync(newJob);

            var returnedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{newJob.Id}.json");

            // Assert the returned job is not null.
            Assert.NotNull(returnedJob);

            // the new job is stored in job store
            Assert.Equal(newJob.ToString(), returnedJob.ToString());
        }

        [Fact]
        public async Task GivenAnActiveJob_WhenUpdateJob_JobShouldBeUpdated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.New,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);
            await blobClient.CreateJob(activeJob);

            var returnedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            Assert.NotNull(returnedJob);
            Assert.Equal(activeJob.ToString(), returnedJob.ToString());

            activeJob.Status = JobStatus.Running;
            activeJob.TotalResourceCounts["Patient"] = 100;
            activeJob.ProcessedResourceCounts["Patient"] = 100;
            activeJob.ProcessedResourceCounts["Patient_customized"] = 95;
            activeJob.SkippedResourceCounts["Patient"] = 0;
            activeJob.SkippedResourceCounts["Patient_customized"] = 5;

            var context = TaskContext.CreateFromJob(
                activeJob,
                _testResourceTypeFilters);

            context.SearchCount = new Dictionary<string, int>() { { "Patient", 10 }, { "Patient_customized", 10 } };
            context.SkippedCount = new Dictionary<string, int>() { { "Patient", 0 }, { "Patient_customized", 0 } };
            context.ProcessedCount = new Dictionary<string, int>() { { "Patient", 10 }, { "Patient_customized", 10 } };
            context.OutputFileIndexMap = new Dictionary<string, int>() { { "Patient", 1 }, { "Patient_customized", 1 } };
            context.SearchProgress.ContinuationToken = "exampleContinuationToken";

            activeJob.RunningTasks[context.Id] = context;
            activeJob.NextTaskIndex = 1;

            await jobStore.UpdateJobAsync(activeJob);

            var updatedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");

            // Assert job is updated.
            Assert.Equal(activeJob.ToString(), updatedJob.ToString());

            Assert.Equal(activeJob.Id, updatedJob.Id);
            Assert.Equal(JobStatus.Running, updatedJob.Status);
            Assert.Equal(1, updatedJob.NextTaskIndex);
            Assert.Equal(100, updatedJob.TotalResourceCounts["Patient"]);
            Assert.Equal(100, updatedJob.ProcessedResourceCounts["Patient"]);
            Assert.Equal(95, updatedJob.ProcessedResourceCounts["Patient_customized"]);
            Assert.Equal(0, updatedJob.SkippedResourceCounts["Patient"]);
            Assert.Equal(5, updatedJob.SkippedResourceCounts["Patient_customized"]);

            Assert.True(updatedJob.RunningTasks.ContainsKey(context.Id));
            Assert.Equal(context.ToString(), updatedJob.RunningTasks[context.Id].ToString());
            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenAnActiveJob_WhenCompleteJob_ArgumentExceptionShouldBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Running,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);
            await blobClient.CreateJob(activeJob);

            await Assert.ThrowsAsync<ArgumentException>(() => jobStore.CompleteJobAsync(activeJob));
            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenASucceededJob_WhenCompleteJob_CompletedJobShouldPresent()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Running,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);
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
            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Running,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);
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

            // job has been added to failedJobs
            var stream = await blobClient.GetBlobAsync($"{AzureBlobJobConstants.SchedulerMetadataFileName}");
            using var streamReader = new StreamReader(stream);
            var metadata = JsonConvert.DeserializeObject<SchedulerMetadata>(streamReader.ReadToEnd());
            Assert.Equal(activeJob.Id, metadata.FailedJobs.First().Id);
            Assert.Equal(activeJob.DataPeriod.Start, metadata.FailedJobs.First().DataPeriod.Start);
            Assert.Equal(activeJob.DataPeriod.End, metadata.FailedJobs.First().DataPeriod.End);
        }

        [Fact]
        public async Task GivenAGroupScopeSucceededJob_WhenCompleteJob_PatientsShouldBeAddedToScheduleMetadata()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var filterInfo = new FilterInfo(FilterScope.Group, "groupId", _testStartTime, _testResourceTypeFilters, null);
            var patients = new List<PatientWrapper>
                { new PatientWrapper("patientId1", 1), new PatientWrapper("patientId2", 0) };

            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Running,
                new DataPeriod(_testStartTime, _testEndTime),
                filterInfo,
                patients);
            await blobClient.CreateJob(activeJob);

            // job has been persisted to active folder.
            var persistedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            Assert.NotNull(persistedJob);

            activeJob.PatientVersionId["patientId1"] = 1;
            activeJob.Status = JobStatus.Succeeded;
            await jobStore.CompleteJobAsync(activeJob);

            // job has been moved to completed folder.
            Assert.Null(await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json"));
            var completedJob = await LoadJobFromBlob(blobClient, $"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.Equal(activeJob.Id, completedJob.Id);

            using var streamReader = new StreamReader(await blobClient.GetBlobAsync($"{AzureBlobJobConstants.SchedulerMetadataFileName}"));
            var metadata = JsonConvert.DeserializeObject<SchedulerMetadata>(streamReader.ReadToEnd());
            Assert.Single(metadata.ProcessedPatients);
            Assert.True(metadata.ProcessedPatients.ContainsKey("patientId1"));
            Assert.Equal(1, metadata.ProcessedPatients["patientId1"]);
            Assert.False(metadata.ProcessedPatients.ContainsKey("patientId2"));

            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenAGroupScopeFailedJob_WhenCompleteJob_PatientsShouldNotBeAddedToScheduleMetadata()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var filterInfo = new FilterInfo(FilterScope.Group, "groupId", _testStartTime, _testResourceTypeFilters, null);
            var patients = new List<PatientWrapper>
                { new PatientWrapper("patientId1", 1), new PatientWrapper("patientId2", 0) };

            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Running,
                new DataPeriod(_testStartTime, _testEndTime),
                filterInfo,
                patients);
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

            // job has been added to failedJobs
            using var streamReader = new StreamReader(await blobClient.GetBlobAsync($"{AzureBlobJobConstants.SchedulerMetadataFileName}"));
            var metadata = JsonConvert.DeserializeObject<SchedulerMetadata>(streamReader.ReadToEnd());
            Assert.Empty(metadata.ProcessedPatients);
            Assert.Equal(activeJob.Id, metadata.FailedJobs.First().Id);
            Assert.Equal(activeJob.DataPeriod.Start, metadata.FailedJobs.First().DataPeriod.Start);
            Assert.Equal(activeJob.DataPeriod.End, metadata.FailedJobs.First().DataPeriod.End);

            jobStore.Dispose();
        }

        [Fact]
        public async Task GivenJobDataStaged_WhenCommit_CorrectResultShouldBeCommitted()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Succeeded,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);

            // Upload parquet data
            var jobStore = CreateInMemoryJobStore(blobClient);
            var data = Encoding.UTF8.GetBytes("test");
            var stageBlobList = new List<string>
            {
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/01/Patient_0000000000_0000000000.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient_customized/2020/11/01/Patient_customized_0000000000_0000000000.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/02/Patient_0000000000_0000000001.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient_customized/2020/11/02/Patient_customized_0000000000_0000000001.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/03/Patient_0000000000_0000000002.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient_customized/2020/11/03/Patient_customized_0000000000_0000000002.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/04/Patient_0000000000_0000000003.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient_customized/2020/11/04/Patient_customized_0000000000_0000000003.parquet",
            };
            var resultBlobList = new List<string>
            {
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/01/{activeJob.Id}/Patient_0000000000_0000000000.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient_customized/2020/11/01/{activeJob.Id}/Patient_customized_0000000000_0000000000.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/02/{activeJob.Id}/Patient_0000000000_0000000001.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient_customized/2020/11/02/{activeJob.Id}/Patient_customized_0000000000_0000000001.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/03/{activeJob.Id}/Patient_0000000000_0000000002.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient_customized/2020/11/03/{activeJob.Id}/Patient_customized_0000000000_0000000002.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/04/{activeJob.Id}/Patient_0000000000_0000000003.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient_customized/2020/11/04/{activeJob.Id}/Patient_customized_0000000000_0000000003.parquet",
            };

            foreach (var blobName in stageBlobList)
            {
                await blobClient.CreateBlobAsync(blobName, new MemoryStream(data));
            }

            activeJob.NextTaskIndex = 1;
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
            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Succeeded,
                new DataPeriod(_testStartTime, _testEndTime),
                _filterInfo);

            // Upload parquet data
            var jobStore = CreateInMemoryJobStore(blobClient);
            var data = Encoding.UTF8.GetBytes("test");
            var stageBlobList = new List<string>
            {
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/01/Patient_0000000000_0000000000.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/02/Patient_0000000000_0000000001.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/03/Patient_0000000001_0000000000.parquet",
                $"{AzureStorageConstants.StagingFolderName}/{activeJob.Id}/Patient/2020/11/04/Patient_0000000002_0000000001.parquet",
            };
            var resultBlobList = new List<string>
            {
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/01/{activeJob.Id}/Patient_0000000000_0000000000.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/02/{activeJob.Id}/Patient_0000000000_0000000001.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/03/{activeJob.Id}/Patient_0000000001_0000000000.parquet",
                $"{AzureStorageConstants.ResultFolderName}/Patient/2020/11/04/{activeJob.Id}/Patient_0000000002_0000000001.parquet",
            };

            foreach (var blobName in stageBlobList)
            {
                await blobClient.CreateBlobAsync(blobName, new MemoryStream(data));
            }

            // Only blobs of taskIndex 0 and 1 should be kept.
            activeJob.NextTaskIndex = 2;
            await jobStore.CompleteJobAsync(activeJob);

            // Make sure data has been moved to result folder.
            Assert.NotNull(await blobClient.GetBlobAsync(resultBlobList[0]));
            Assert.NotNull(await blobClient.GetBlobAsync(resultBlobList[1]));
            Assert.NotNull(await blobClient.GetBlobAsync(resultBlobList[2]));
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
