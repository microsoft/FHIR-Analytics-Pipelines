// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Azure;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Fhir;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Scheduler.Exceptions;
using Microsoft.Health.Fhir.Synapse.Scheduler.Jobs;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.UnitTests.Jobs
{
    [Trait("Category", "Scheduler")]

    public class InMemoryJobStoreTests
    {
        private const string ContainerName = "TestJobContainer";

        [Fact]
        public async Task GivenNoJobRunning_WhenStartJob_NewJobShouldBeCreated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            _ = await jobStore.StartJobAsync();

            var job = await blobClient.GetValue<Job>(JobConstants.RunningJobName);
            Assert.Equal(ContainerName, job.ContainerName);
            Assert.Equal(DateTimeOffset.MinValue, job.DataPeriod.Start);
            Assert.Equal(2, job.ResourceTypes.Count());
        }

        [Fact]
        public async Task GivenAJobRunning_WhenStartJob_NoJobShouldBeCreated()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);
            var runningJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now);
            await blobClient.CreateJob(runningJob, JobConstants.RunningJobName, "SampleLease");

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() =>
                jobStore.StartJobAsync());
            Assert.Equal(JobErrorCode.ResumeJobConflict, exception.Code);
        }

        [Fact]
        public async Task GivenAnInactiveJobRunning_WhenStartJob_JobShouldBeResumed()
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
            await blobClient.CreateJob(runningJob, JobConstants.RunningJobName);
            var resumedJob = await jobStore.StartJobAsync();

            Assert.Equal(ContainerName, resumedJob.ContainerName);
            Assert.Equal(runningJob.DataPeriod.Start, resumedJob.DataPeriod.Start);
            Assert.Equal(runningJob.DataPeriod.End, resumedJob.DataPeriod.End);
            Assert.Equal(runningJob.ResourceTypes, resumedJob.ResourceTypes);
        }

        [Fact]
        public async Task GivenALeasedActiveJobRunning_WhenStartJob_NoJobShouldBeResumed()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            // Add an running job that was active 8 minutes ago.
            var runningJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-8));
            await blobClient.CreateJob(runningJob, JobConstants.RunningJobName, "SampleLease");

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() =>
                jobStore.StartJobAsync());
            Assert.Equal(JobErrorCode.ResumeJobConflict, exception.Code);
        }

        [Fact]
        public async Task GivenALeasedInactiveJobRunning_WhenStartJob_NoJobShouldBeResumed()
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
            await blobClient.CreateJob(runningJob, JobConstants.RunningJobName);
            var resumedJob = await jobStore.StartJobAsync();

            Assert.Equal(ContainerName, resumedJob.ContainerName);
            Assert.Equal(runningJob.DataPeriod.Start, resumedJob.DataPeriod.Start);
            Assert.Equal(runningJob.DataPeriod.End, resumedJob.DataPeriod.End);
            Assert.Equal(runningJob.ResourceTypes, resumedJob.ResourceTypes);
        }

        [Fact]
        public async Task GivenASucceededJob_WhenStartJob_NoJobShouldBeResumed()
        {
            var blobClient = new InMemoryBlobContainerClient();
            var jobStore = CreateInMemoryJobStore(blobClient);

            // Add an running job that was active 8 minutes ago.
            var runningJob = new Job(
                ContainerName,
                JobStatus.Completed,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-8));
            await blobClient.CreateJob(runningJob, JobConstants.RunningJobName, "SampleLease");

            var exception = await Assert.ThrowsAsync<StartJobFailedException>(() =>
                jobStore.StartJobAsync());
            Assert.Equal(JobErrorCode.NoJobToSchedule, exception.Code);
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
    }
}
