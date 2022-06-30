﻿// -------------------------------------------------------------------------------------------------
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
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
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

        private static readonly List<TypeFilter> _testResourceTypeFilters =
            new List<TypeFilter> {new ("Patient", null), new ("Observation", null) };

        private static readonly FilterContext _filterContext = new FilterContext(JobScope.System, null, _testResourceTypeFilters, null);

        private static readonly FhirParquetSchemaManager _fhirSchemaManager;

        static InMemoryJobManagerTests()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = TestUtils.DefaultSchemaDirectoryPath,
            });

            _fhirSchemaManager = new FhirParquetSchemaManager(schemaConfigurationOption, NullLogger<FhirParquetSchemaManager>.Instance);
        }

        [Fact]
        public async Task GivenABrokenJobStore_WhenExecute_ExceptionShouldBeThrown()
        {
            var jobManager = CreateJobManager(CreateBrokenJobStore(), _testStartTime, _testEndTime);
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
            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Running,
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.MinValue,
                _filterContext);

            await blobClient.CreateJob(activeJob, "SampleLease");

            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime);

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

            // Create an active job.
            var activeJob = Job.Create(
                TestContainerName,
                JobStatus.Running,
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.MinValue,
                _filterContext);
            await blobClient.CreateJob(activeJob, "SampleLease");

            await blobClient.ReleaseLeaseAsync(AzureBlobJobConstants.JobLockFileName, "SampleLease");

            // the job has been uploaded to job store.
            var uploadedJob = await blobClient.GetValue<Job>($"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            Assert.NotNull(uploadedJob);

            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime);
            await jobManager.RunAsync();

            // The running job has been resumed and completed.
            var job = await blobClient.GetValue<Job>($"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            Assert.Null(job);
            var completedJob = await blobClient.GetValue<Job>($"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.NotNull(completedJob);
            Assert.Equal(JobStatus.Succeeded, completedJob.Status);

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
            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.ActiveJobFolder));
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

            var failedJob = Job.Create(
                TestContainerName,
                JobStatus.Failed,
                new DataPeriod(_testStartTime, _testEndTime),
                DateTimeOffset.MinValue,
                _filterContext);

            var metadata = new SchedulerMetadata();
            metadata.UnfinishedJobs = new List<Job>()
            {
                failedJob,
            };

            await blobClient.CreateBlobAsync(
                AzureBlobJobConstants.SchedulerMetadataFileName,
                new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(metadata))));
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);
            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.ActiveJobFolder));
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
        public async Task
            GivenNoSchedulerMetadata_WhenExecuteGroupScope_NewJobWillBeTriggeredAndPatientsWillBeAddedToSchedulerMetadata()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
            };

            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);
            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime, JobScope.Group);

            var schedulerBlob = await blobClient.GetBlobAsync($"{AzureBlobJobConstants.SchedulerMetadataFileName}");
            Assert.Null(schedulerBlob);

            await jobManager.RunAsync();

            using var streamReader = new StreamReader(await blobClient.GetBlobAsync($"{AzureBlobJobConstants.SchedulerMetadataFileName}"));
            var newMetadata = JsonConvert.DeserializeObject<SchedulerMetadata>(streamReader.ReadToEnd());
            Assert.Equal(_testEndTime, newMetadata.LastScheduledTimestamp);
            Assert.Empty(newMetadata.UnfinishedJobs);
            Assert.Contains("patientId1", newMetadata.ProcessedPatientIds);
            Assert.Contains("patientId2", newMetadata.ProcessedPatientIds);

            jobManager.Dispose();
        }

        [Fact]
        public async Task
            GivenSchedulerMetadata_WhenExecuteGroupScope_NewJobWillBeTriggeredAndPatientsWillBeAddedToSchedulerMetadata()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
            };

            var metadata = new SchedulerMetadata()
            {
                ProcessedPatientIds = new List<string>{"patientId0", "patientId1"},
            };

            await blobClient.CreateBlobAsync(
                AzureBlobJobConstants.SchedulerMetadataFileName,
                new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(metadata))));

            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);
            var jobManager = CreateJobManager(jobStore, _testStartTime, _testEndTime, JobScope.Group);

            await jobManager.RunAsync();

            using var streamReader = new StreamReader(await blobClient.GetBlobAsync($"{AzureBlobJobConstants.SchedulerMetadataFileName}"));
            var newMetadata = JsonConvert.DeserializeObject<SchedulerMetadata>(streamReader.ReadToEnd());
            Assert.Equal(_testEndTime, newMetadata.LastScheduledTimestamp);
            Assert.Empty(newMetadata.UnfinishedJobs);
            Assert.Contains("patientId0", newMetadata.ProcessedPatientIds);
            Assert.Contains("patientId1", newMetadata.ProcessedPatientIds);
            Assert.Contains("patientId2", newMetadata.ProcessedPatientIds);

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
            var jobManager = CreateJobManager(jobStore, _testStartTime, DateTimeOffset.MaxValue);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var triggeredStartTime = DateTimeOffset.UtcNow;
            await jobManager.RunAsync();
            var triggeredEndTime = DateTimeOffset.UtcNow;
            Assert.NotEmpty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var blobName = (await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder)).First();
            var blob = await blobClient.GetBlobAsync(blobName);
            using var streamReader = new StreamReader(blob);
            var job = JsonConvert.DeserializeObject<Job>(streamReader.ReadToEnd());

            Assert.Equal(JobStatus.Succeeded, job.Status);
            Assert.Equal(_testStartTime, job.DataPeriod.Start);

            // Test end data period
            Assert.True(job.DataPeriod.End > triggeredStartTime.AddMinutes(-2));
            Assert.True(job.DataPeriod.End < triggeredEndTime.AddMinutes(-2));

            jobManager.Dispose();
        }

        [Fact]
        public async Task GivenInvalidTimeRange_WhenExecute_ExceptionWillBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
            };
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);

            var jobManager = CreateJobManager(jobStore, DateTimeOffset.UtcNow.AddDays(1), DateTimeOffset.MaxValue);

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
            var jobManager = CreateJobManager(jobStore, _testEndTime, _testEndTime);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.RunAsync());
            Assert.Contains("Job has been scheduled to end", exception.Message);

            jobManager.Dispose();
        }

        [Fact]
        public async Task GivenBrokenJobExecutor_WhenExecute_ExceptionWillBeThrown()
        {
            var jobConfig = new JobConfiguration()
            {
                ContainerName = TestContainerName,
                StartTime = _testStartTime,
                EndTime = _testEndTime,
            };

            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);

            var filterConfiguration = new FilterConfiguration
            {
                JobScope = JobScope.System,
            };

            var typeFilterParser = Substitute.For<ITypeFilterParser>();
            typeFilterParser.CreateTypeFilters(Arg.Any<JobScope>(), Arg.Any<string>(), Arg.Any<string>()).Returns(_testResourceTypeFilters);

            var taskExecutor = Substitute.For<ITaskExecutor>();
            taskExecutor.ExecuteAsync(Arg.Any<TaskContext>(), Arg.Any<JobProgressUpdater>(), Arg.Any<CancellationToken>()).Returns(CreateTestTaskResult());

            var jobExecutor = Substitute.For<IJobExecutor>();
            jobExecutor.ExecuteAsync(default, default).ReturnsForAnyArgs(Task.FromException(new Exception()));

            var jobManager = new JobManager(
                jobStore,
                jobExecutor,
                typeFilterParser,
                Options.Create<JobConfiguration>(jobConfig),
                Options.Create<FilterConfiguration>(filterConfiguration),
                new NullLogger<JobManager>());

            await Assert.ThrowsAsync<Exception>(() => jobManager.RunAsync());

            jobStore.Dispose();
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
            return new TaskResult(
                true,
                new Dictionary<string, int>() { { "Patient", 100 } },
                new Dictionary<string, int>() { { "Patient", 0 } },
                new Dictionary<string, int>() { { "Patient", 100 } },
                string.Empty);
        }

        private JobManager CreateJobManager(
            IJobStore jobStore,
            DateTimeOffset start,
            DateTimeOffset end,
            JobScope jobScope = JobScope.System)
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
            };

            var filterConfiguration = new FilterConfiguration
            {
                JobScope = jobScope,
            };

            var typeFilterParser = Substitute.For<ITypeFilterParser>();
            typeFilterParser.CreateTypeFilters(Arg.Any<JobScope>(), Arg.Any<string>(), Arg.Any<string>()).Returns(_testResourceTypeFilters);

            var groupMemberExtractor = Substitute.For<IGroupMemberExtractor>();

            var patients = new List<PatientWrapper> { new PatientWrapper("patientId1"), new PatientWrapper("patientId2") };

            groupMemberExtractor.GetGroupPatients(default, default, default, default).ReturnsForAnyArgs(patients);

            var taskExecutor = Substitute.For<ITaskExecutor>();
            taskExecutor.ExecuteAsync(Arg.Any<TaskContext>(), Arg.Any<JobProgressUpdater>(), Arg.Any<CancellationToken>()).Returns(CreateTestTaskResult());
            var jobExecutor = new JobExecutor(taskExecutor, new JobProgressUpdaterFactory(jobStore, new NullLoggerFactory()), groupMemberExtractor, Options.Create(schedulerConfig), new NullLogger<JobExecutor>());

            return new JobManager(
                jobStore,
                jobExecutor,
                typeFilterParser,
                Options.Create<JobConfiguration>(jobConfiguration),
                Options.Create<FilterConfiguration>(filterConfiguration),
                new NullLogger<JobManager>());
        }
    }
}
