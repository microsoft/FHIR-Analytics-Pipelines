﻿// -------------------------------------------------------------------------------------------------
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
using Microsoft.Health.Fhir.Synapse.Azure.Blob;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Fhir;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Scheduler.Exceptions;
using Microsoft.Health.Fhir.Synapse.Scheduler.Jobs;
using Microsoft.Health.Fhir.Synapse.Scheduler.Tasks;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.UnitTests.Jobs
{
    [Trait("Category", "Scheduler")]
    public class InMemoryJobManagerTests
    {
        private const string ContainerName = "jobunittests";

        [Fact]
        public async Task GivenABrokenJobStore_WhenExecute_ExceptionShouldBeThrown()
        {
            var jobManager = CreateJobManager(CreateBrokenJobStore());
            await Assert.ThrowsAsync<Exception>(() => jobManager.TriggerJobAsync());
        }

        [Fact]
        public async Task GivenALeasedActiveJobRunning_WhenExecute_NoJobWillBeCreated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var activeJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-5));
            await blobClient.CreateJob(activeJob, "SampleLease");

            var jobManager = CreateJobManager(jobStore);
            await jobManager.TriggerJobAsync();

            // Only one job is running.
            var job = await blobClient.GetValue<Job>($"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            Assert.NotNull(job);

            var activeJobs = await blobClient.ListBlobsAsync(AzureBlobJobConstants.ActiveJobFolder);
            Assert.Single(activeJobs);
        }

        [Fact]
        public async Task GivenJobLockReleased_WhenExecute_JobWillBeResumed()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            var activeJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));
            await blobClient.CreateJob(activeJob, "SampleLease");

            await blobClient.ReleaseLeaseAsync(AzureBlobJobConstants.JobLockFileName, "SampleLease");

            var jobManager = CreateJobManager(jobStore);
            await jobManager.TriggerJobAsync();

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
            var jobStore = CreateInMemoryJobStore(blobClient);
            var jobManager = CreateJobManager(jobStore);

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            await jobManager.TriggerJobAsync();
            Assert.NotEmpty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var blobName = (await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder)).First();
            var blob = await blobClient.GetBlobAsync(blobName);
            using var streamReader = new StreamReader(blob);
            var job = JsonConvert.DeserializeObject<Job>(streamReader.ReadToEnd());

            Assert.Equal(JobStatus.Completed, job.Status);
            Assert.Equal(DateTimeOffset.MinValue, job.DataPeriod.Start);

            // Test end data period
            Assert.True(job.DataPeriod.End < DateTimeOffset.UtcNow.AddMinutes(-1 * AzureBlobJobConstants.JobQueryLatencyInMinutes));
            Assert.True(job.DataPeriod.End > DateTimeOffset.UtcNow.AddMinutes(-1 * AzureBlobJobConstants.JobQueryLatencyInMinutes).AddMinutes(-1));
        }

        [Fact]
        public async Task GivenInvalidTimeRange_WhenExecute_ExceptionWillBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var jobManager = CreateJobManager(jobStore, new JobConfiguration
            {
                ContainerName = ContainerName,
                StartTime = DateTimeOffset.UtcNow.AddDays(1),
                ResourceTypeFilters = new List<string> { "Patient", "Observation" },
            });

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.TriggerJobAsync());
            Assert.Contains("trigger is in the future", exception.Message);
        }

        [Fact]
        public async Task GivenCompletedTimeRange_WhenExecute_ExceptionWillBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var time = DateTimeOffset.UtcNow.AddDays(-1);
            var jobManager = CreateJobManager(jobStore, new JobConfiguration
            {
                ContainerName = ContainerName,
                StartTime = time,
                EndTime = time,
                ResourceTypeFilters = new List<string> { "Patient", "Observation" },
            });

            Assert.Empty(await blobClient.ListBlobsAsync(AzureBlobJobConstants.CompletedJobFolder));

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.TriggerJobAsync());
            Assert.Contains("Job has been scheduled to end", exception.Message);
        }

        private IJobStore CreateBrokenJobStore()
        {
            var jobStore = Substitute.For<IJobStore>();
            jobStore.AcquireJobLock().Returns(Task.FromException<bool>(new Exception()));
            return jobStore;
        }

        private AzureBlobJobStore CreateInMemoryJobStore(IAzureBlobContainerClient blobClient)
        {
            var jobConfiguration = new JobConfiguration
            {
                ContainerName = ContainerName,
                StartTime = DateTimeOffset.MinValue,
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

        private JobManager CreateJobManager(IJobStore jobStore, JobConfiguration config = null)
        {
            var jobConfiguration = config;
            if (jobConfiguration == null)
            {
                jobConfiguration = new JobConfiguration
                {
                    ContainerName = ContainerName,
                    StartTime = DateTimeOffset.MinValue,
                    ResourceTypeFilters = new List<string> { "Patient", "Observation" },
                };
            }

            var schedulerConfig = new JobSchedulerConfiguration
            {
                MaxConcurrencyCount = 5,
            };

            var taskExecutor = Substitute.For<ITaskExecutor>();
            taskExecutor.ExecuteAsync(Arg.Any<TaskContext>(), Arg.Any<IProgress<TaskContext>>(), Arg.Any<CancellationToken>()).Returns(new TaskResult());
            return new JobManager(
                jobStore,
                taskExecutor,
                new R4FhirSpecificationProvider(),
                Options.Create(schedulerConfig),
                Options.Create(jobConfiguration),
                new NullLogger<JobManager>());

        }
    }
}
