// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient.Fhir;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    [Trait("Category", "Job")]
    public class InMemoryJobManagerTests
    {
        private const string ContainerName = "TestJobContainer";

        [Fact]
        public async Task GivenABrokenJobStore_WhenExecute_ExceptionShouldBeThrown()
        {
            var jobManager = CreateJobManager(CreateBrokenJobStore());
            await Assert.ThrowsAsync<Exception>(() => jobManager.RunAsync());
        }

        [Fact]
        public async Task GivenALeasedActiveJobRunning_WhenExecute_NoJobWillBeCreated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                StartTime = DateTimeOffset.UtcNow.AddDays(-2),
                EndTime = DateTimeOffset.UtcNow,
                ResourceTypeFilters = new List<string> { "Patient" },
            };
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);

            // Create new job.
            var job = await jobStore.AcquireJobAsync();
            var jobManager = CreateJobManager(jobStore);

            await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.RunAsync());

            // Only one job is running.
            var activeJobs = await blobClient.ListBlobsAsync(AzureBlobJobConstants.ActiveJobFolder);
            Assert.Single(activeJobs);
        }

        [Fact]
        public async Task GivenJobLockReleased_WhenExecute_JobWillBeResumed()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                StartTime = DateTimeOffset.UtcNow.AddDays(-2),
                EndTime = DateTimeOffset.UtcNow,
                ResourceTypeFilters = new List<string> { "Patient" },
            };
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);

            var activeJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));
            await blobClient.CreateJob(activeJob, "SampleLease");

            await blobClient.ReleaseLeaseAsync(AzureBlobJobConstants.JobLockFileName, "SampleLease");

            var jobManager = CreateJobManager(jobStore);
            await jobManager.RunAsync();

            // The running job has been resumed and completed.
            var job = await blobClient.GetValue<Job>($"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            Assert.Null(job);
            var completedJob = await blobClient.GetValue<Job>($"{AzureBlobJobConstants.CompletedJobFolder}/{activeJob.Id}.json");
            Assert.NotNull(completedJob);
        }

        [Fact]
        public async Task GivenNoActiveJob_WhenExecute_NewJobWillBeTriggered()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobConfig = new JobConfiguration()
            {
                StartTime = DateTimeOffset.UtcNow.AddDays(-2),
                EndTime = DateTimeOffset.UtcNow,
                ResourceTypeFilters = new List<string> { "Patient" },
            };
            var jobStore = CreateInMemoryJobStore(blobClient, jobConfig);
            var jobManager = CreateJobManager(jobStore);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            await jobManager.RunAsync();
            Assert.NotEmpty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var blobName = (await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder)).First();
            var blob = await blobClient.GetBlobAsync(blobName);
            using var streamReader = new StreamReader(blob);
            var job = JsonConvert.DeserializeObject<Job>(streamReader.ReadToEnd());

            Assert.Equal(JobStatus.Succeeded, job.Status);
            Assert.Equal(jobConfig.StartTime, job.DataPeriod.Start);

            // Test end data period
            Assert.True(job.DataPeriod.End < DateTimeOffset.UtcNow.AddMinutes(-1 * AzureBlobJobConstants.JobQueryLatencyInMinutes));
            Assert.True(job.DataPeriod.End > DateTimeOffset.UtcNow.AddMinutes(-1 * AzureBlobJobConstants.JobQueryLatencyInMinutes).AddMinutes(-1));
        }

        [Fact]
        public async Task GivenInvalidTimeRange_WhenExecute_ExceptionWillBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient, new JobConfiguration
            {
                ContainerName = ContainerName,
                StartTime = DateTimeOffset.UtcNow.AddDays(1),
                ResourceTypeFilters = new List<string> { "Patient", "Observation" },
            });
            var jobManager = CreateJobManager(jobStore);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.RunAsync());
            Assert.Contains("trigger is in the future", exception.Message);
        }

        [Fact]
        public async Task GivenCompletedTimeRange_WhenExecute_ExceptionWillBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var time = DateTimeOffset.UtcNow.AddDays(-1);
            var jobStore = CreateInMemoryJobStore(blobClient, new JobConfiguration
            {
                ContainerName = ContainerName,
                StartTime = time,
                EndTime = time,
                ResourceTypeFilters = new List<string> { "Patient", "Observation" },
            });
            var jobManager = CreateJobManager(jobStore);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.RunAsync());
            Assert.Contains("Job has been scheduled to end", exception.Message);
        }

        private static IJobStore CreateBrokenJobStore()
        {
            var jobStore = Substitute.For<IJobStore>();
            jobStore.AcquireJobAsync().Returns(Task.FromException<Job>(new Exception()));
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
                new R4FhirSpecificationProvider(),
                Options.Create(config),
                Options.Create(storeConfiguration),
                new NullLogger<AzureBlobJobStore>());
        }

        private static TaskResult CreateTestTaskResult()
        {
            return new TaskResult("Patient", null, 0, 100, 0, 100, string.Empty);
        }

        private JobManager CreateJobManager(IJobStore jobStore)
        {
            var schedulerConfig = new JobSchedulerConfiguration
            {
                MaxConcurrencyCount = 5,
            };

            var taskExecutor = Substitute.For<ITaskExecutor>();
            taskExecutor.ExecuteAsync(Arg.Any<TaskContext>(), Arg.Any<JobProgressUpdater>(), Arg.Any<CancellationToken>()).Returns(CreateTestTaskResult());
            var jobExecutor = new JobExecutor(taskExecutor, new JobProgressUpdaterFactory(jobStore), Options.Create(schedulerConfig), new NullLogger<JobExecutor>());

            return new JobManager(
                jobStore,
                jobExecutor,
                new NullLogger<JobManager>());
        }
    }
}
