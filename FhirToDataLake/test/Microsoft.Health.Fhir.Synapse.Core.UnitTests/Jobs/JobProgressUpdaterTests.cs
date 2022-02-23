// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class JobProgressUpdaterTests
    {
        public static string ContainerName { get; private set; } = "progressupdatertests";

        [Fact]
        public async Task GivenNoContextUpdates_WhenUpdateJobProgress_NoProgressShouldBeUpdated()
        {
            var activeJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));

            var containerClient = new InMemoryBlobContainerClient();
            var jobProgressUpdater = GetInMemoryJobProgressUpdater(activeJob, containerClient);
            var updateTask = Task.Run(() => jobProgressUpdater.Consume());
            jobProgressUpdater.Complete();
            await updateTask;

            foreach (var resource in activeJob.ResourceProgresses.Keys)
            {
                Assert.Null(activeJob.ResourceProgresses[resource]);
                Assert.Equal(0, activeJob.TotalResourceCounts[resource]);
                Assert.Equal(0, activeJob.ProcessedResourceCounts[resource]);
                Assert.Equal(0, activeJob.SkippedResourceCounts[resource]);
            }

            var persistedJob = await containerClient.GetValue<Job>($"jobs/activeJobs/{activeJob.Id}.json");
            foreach (var resource in persistedJob.ResourceProgresses.Keys)
            {
                Assert.Null(persistedJob.ResourceProgresses[resource]);
                Assert.Equal(0, persistedJob.TotalResourceCounts[resource]);
                Assert.Equal(0, persistedJob.ProcessedResourceCounts[resource]);
                Assert.Equal(0, persistedJob.SkippedResourceCounts[resource]);
            }
        }

        [Fact]
        public async Task GivenContextUpdates_WhenUpdateJobProgress_ProgressShouldBeUpdatedCorrectly()
        {
            var activeJob = new Job(
                ContainerName,
                JobStatus.Running,
                new List<string> { "Patient", "Observation" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));
            var context = new TaskContext(
                "test",
                activeJob.Id,
                "Patient",
                activeJob.DataPeriod.Start,
                activeJob.DataPeriod.End,
                "exampleContinuationToken",
                10,
                10,
                0,
                1);

            var containerClient = new InMemoryBlobContainerClient();
            var jobProgressUpdater = GetInMemoryJobProgressUpdater(activeJob, containerClient);
            var updateTask = Task.Run(() => jobProgressUpdater.Consume());
            await jobProgressUpdater.Produce(context);
            jobProgressUpdater.Complete();
            await updateTask;

            Assert.Equal("exampleContinuationToken", activeJob.ResourceProgresses["Patient"]);
            Assert.Equal(10, activeJob.TotalResourceCounts["Patient"]);
            Assert.Equal(10, activeJob.ProcessedResourceCounts["Patient"]);
            Assert.Equal(0, activeJob.SkippedResourceCounts["Patient"]);

            var persistedJob = await containerClient.GetValue<Job>($"jobs/activeJobs/{activeJob.Id}.json");
            Assert.Equal("exampleContinuationToken", persistedJob.ResourceProgresses["Patient"]);
            Assert.Equal(10, persistedJob.TotalResourceCounts["Patient"]);
            Assert.Equal(10, persistedJob.ProcessedResourceCounts["Patient"]);
            Assert.Equal(0, persistedJob.SkippedResourceCounts["Patient"]);
        }

        public static JobProgressUpdater GetInMemoryJobProgressUpdater(Job job, IAzureBlobContainerClient containerClient)
        {
            var jobConfiguration = new JobConfiguration
            {
                ContainerName = "jobprogressupdater",
                StartTime = DateTimeOffset.MinValue,
                EndTime = DateTimeOffset.MaxValue,
                ResourceTypeFilters = new List<string> { "Patient", "Observation" },
            };

            var storeConfiguration = new DataLakeStoreConfiguration
            {
                StorageUrl = "http://test.blob.core.windows.net",
            };

            var mockFactory = Substitute.For<IAzureBlobContainerClientFactory>();
            mockFactory.Create(Arg.Any<string>(), Arg.Any<string>()).ReturnsForAnyArgs(containerClient);
            var jobStore = new AzureBlobJobStore(
                mockFactory,
                Options.Create(jobConfiguration),
                Options.Create(storeConfiguration),
                new NullLogger<AzureBlobJobStore>());

            return new JobProgressUpdater(jobStore, job, new NullLogger<JobProgressUpdater>());
        }
    }
}
