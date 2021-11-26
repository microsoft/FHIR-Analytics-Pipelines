// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Azure;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Fhir;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Scheduler.Exceptions;
using Microsoft.Health.Fhir.Synapse.Scheduler.Jobs;
using Microsoft.Health.Fhir.Synapse.Scheduler.Tasks;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.UnitTests.Jobs
{
    [Trait("Category", "Scheduler")]
    public class InMemoryJobManagerTests
    {
        private const string ContainerName = "TestJobContainer";

        [Fact]
        public async Task GivenABrokenJobStore_WhenExecute_ExceptionShouldBeThrown()
        {
            var jobManager = CreateJobManager(CreateBrokenJobStore());
            await Assert.ThrowsAsync<StartJobFailedException>(() => jobManager.ExecuteAsync());
        }

        [Fact]
        public async Task GivenALeasedActiveJobRunning_WhenExecute_NoJobWillBeCreated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            // Add an running job that was active 5 minutes ago.
            var runningJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-5));
            await blobClient.CreateJob(runningJob, JobConstants.RunningJobName, "SampleLease");

            var jobManager = CreateJobManager(jobStore);
            await jobManager.ExecuteAsync();

            // Job heartbeat is not changed.
            var job = await blobClient.GetValue<Job>(JobConstants.RunningJobName);
            Assert.Equal(runningJob.LastHeartBeat, job.LastHeartBeat);
        }

        [Fact]
        public async Task GivenALeasedInactiveJobRunning_WhenExecute_JobWillBeResumed()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            // Add an running job that was active 11 minutes ago.
            var runningJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));
            await blobClient.CreateJob(runningJob, JobConstants.RunningJobName, "SampleLease");

            var jobManager = CreateJobManager(jobStore);
            await jobManager.ExecuteAsync();

            // The running job has been resumed and completed.
            var job = await blobClient.GetValue<Job>(JobConstants.RunningJobName);
            Assert.Null(job);
            var completedJob = await blobClient.GetValue<Job>($"{JobConstants.CompletedJobFolder}/{runningJob.Id}");
            Assert.NotNull(completedJob);
        }

        private IJobStore CreateBrokenJobStore()
        {
            var jobStore = Substitute.For<IJobStore>();
            jobStore.StartJobAsync().Returns(x => Task.FromException<Job>(new Exception()));
            return jobStore;
        }

        private JobStore CreateInMemoryJobStore(IAzureBlobContainerClient blobClient)
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
            return new JobStore(
                mockFactory,
                new R4FhirSpecificationProvider(),
                Options.Create(jobConfiguration),
                Options.Create(storeConfiguration),
                new NullLogger<JobStore>());
        }

        private JobManager CreateJobManager(IJobStore jobStore)
        {
            var jobConfiguration = new JobConfiguration
            {
                ContainerName = ContainerName,
                StartTime = DateTimeOffset.MinValue,
                EndTime = DateTimeOffset.MaxValue,
                ResourceTypeFilters = new List<string> { "Patient", "Observation" },
            };
            var schedulerConfig = new JobSchedulerConfiguration
            {
                MaxConcurrencyCount = 5,
            };

            var taskExecutor = Substitute.For<ITaskExecutor>();
            taskExecutor.ExecuteAsync(Arg.Any<TaskContext>(), Arg.Any<IProgress<TaskContext>>(), Arg.Any<CancellationToken>()).Returns(new TaskResult());
            return new JobManager(
                jobStore,
                taskExecutor,
                Options.Create(schedulerConfig),
                new NullLogger<JobManager>());

        }
    }
}
