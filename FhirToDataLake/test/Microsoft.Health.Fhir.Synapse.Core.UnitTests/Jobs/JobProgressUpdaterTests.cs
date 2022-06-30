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
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
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
            var typeFilters = new List<TypeFilter> { new ("Patient", null), new ("Observation", null) };
            var filterContext = new FilterContext(JobScope.System, null, typeFilters, null);

            var activeJob = Job.Create(
                ContainerName,
                JobStatus.Running,
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.MinValue,
                filterContext);

            var containerClient = new InMemoryBlobContainerClient();
            var jobProgressUpdater = GetInMemoryJobProgressUpdater(activeJob, containerClient);
            var updateTask = Task.Run(() => jobProgressUpdater.Consume());
            jobProgressUpdater.Complete();
            await updateTask;

            var persistedJob = await containerClient.GetValue<Job>($"jobs/activeJobs/{activeJob.Id}.json");
            Assert.Equal(activeJob.ToString(), persistedJob.ToString());
        }

        [Fact]
        public async Task GivenContextUpdates_WhenUpdateJobProgress_ProgressShouldBeUpdatedCorrectly()
        {
            var typeFilters = new List<TypeFilter> { new ("Patient", null), new ("Observation", null) };
            var filterContext = new FilterContext(JobScope.System, null, typeFilters, null);

            var activeJob = Job.Create(
                ContainerName,
                JobStatus.Running,
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.MinValue,
                filterContext);

            var context1 = TaskContext.Create(
                activeJob,
                activeJob.NextTaskIndex,
                typeFilters);

            var containerClient = new InMemoryBlobContainerClient();
            var jobProgressUpdater = GetInMemoryJobProgressUpdater(activeJob, containerClient);
            var updateTask = Task.Run(() => jobProgressUpdater.Consume());

            activeJob.RunningTasks[context1.Id] = context1;
            activeJob.NextTaskIndex++;

            Assert.True(activeJob.RunningTasks.ContainsKey(context1.Id));
            Assert.Equal(context1.ToString(), activeJob.RunningTasks[context1.Id].ToString());
            Assert.Empty(activeJob.TotalResourceCounts);
            Assert.Empty(activeJob.ProcessedResourceCounts);
            Assert.Empty(activeJob.SkippedResourceCounts);

            // context 1 is updated amd uncompleted
            context1.SearchCount = new Dictionary<string, int>() { {"Patient", 10}};
            context1.SkippedCount = new Dictionary<string, int>() { {"Patient", 0}, {"Patient_customized", 0}};
            context1.ProcessedCount = new Dictionary<string, int>() { {"Patient", 10}, {"Patient_customized", 10}};
            context1.OutputFileIndexMap = new Dictionary<string, int>() { {"Patient", 1}, {"Patient_customized", 1}};
            context1.SearchProgress.ContinuationToken = "exampleContinuationToken";

            Assert.True(activeJob.RunningTasks.ContainsKey(context1.Id));
            Assert.Equal("exampleContinuationToken", activeJob.RunningTasks[context1.Id].SearchProgress.ContinuationToken);
            Assert.Empty(activeJob.TotalResourceCounts);
            Assert.Empty(activeJob.ProcessedResourceCounts);
            Assert.Empty(activeJob.SkippedResourceCounts);

            // sync up the updated context to job
            await jobProgressUpdater.Produce(context1);

            var context2 = TaskContext.Create(
                activeJob,
                activeJob.NextTaskIndex,
                typeFilters);

            activeJob.RunningTasks[context2.Id] = context2;
            activeJob.NextTaskIndex++;

            // context 2 is completed
            context2.SearchCount = new Dictionary<string, int>() { { "Patient", 20 } };
            context2.SkippedCount = new Dictionary<string, int>() { { "Patient", 0 }, { "Patient_customized", 0 } };
            context2.ProcessedCount = new Dictionary<string, int>() { { "Patient", 20 }, { "Patient_customized", 20 } };
            context2.OutputFileIndexMap = new Dictionary<string, int>() { { "Patient", 1 }, { "Patient_customized", 1 } };
            context2.IsCompleted = true;

            await jobProgressUpdater.Produce(context2);

            jobProgressUpdater.Complete();
            await updateTask;

            Assert.True(activeJob.RunningTasks.ContainsKey(context1.Id));
            Assert.False(activeJob.RunningTasks.ContainsKey(context2.Id));
            Assert.Equal("exampleContinuationToken", activeJob.RunningTasks[context1.Id].SearchProgress.ContinuationToken);
            Assert.Single(activeJob.TotalResourceCounts);
            Assert.Equal(20, activeJob.TotalResourceCounts["Patient"]);
            Assert.Equal(20, activeJob.ProcessedResourceCounts["Patient"]);
            Assert.Equal(20, activeJob.ProcessedResourceCounts["Patient_customized"]);
            Assert.Equal(0, activeJob.SkippedResourceCounts["Patient"]);
            Assert.Equal(0, activeJob.SkippedResourceCounts["Patient_customized"]);
        }

        [Fact]
        public async Task GivenTaskUpdateContextTwice_WhenUpdateJobProgress_ProgressShouldBeUpdatedCorrectly()
        {
            var typeFilters = new List<TypeFilter> { new("Patient", null), new("Observation", null) };
            var filterContext = new FilterContext(JobScope.System, null, typeFilters, null);

            var activeJob = Job.Create(
                ContainerName,
                JobStatus.Running,
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.MinValue,
                filterContext);

            var context = TaskContext.Create(
                activeJob,
                0,
                typeFilters);

            var containerClient = new InMemoryBlobContainerClient();
            var jobProgressUpdater = GetInMemoryJobProgressUpdater(activeJob, containerClient);
            var updateTask = Task.Run(() => jobProgressUpdater.Consume());

            activeJob.RunningTasks[context.Id] = context;
            activeJob.NextTaskIndex++;

            // context is updated
            context.SearchCount = new Dictionary<string, int>() { { "Patient", 10 }};
            context.SkippedCount = new Dictionary<string, int>() { { "Patient", 0 }, { "Patient_customized", 0 } };
            context.ProcessedCount = new Dictionary<string, int>() { { "Patient", 10 }, { "Patient_customized", 10 } };
            context.OutputFileIndexMap = new Dictionary<string, int>() { { "Patient", 1 }, { "Patient_customized", 1 } };
            context.SearchProgress.ContinuationToken = "exampleContinuationToken";

            // sync up the updated context to job
            await jobProgressUpdater.Produce(context);

            Assert.True(activeJob.RunningTasks.ContainsKey(context.Id));
            Assert.Equal("exampleContinuationToken", activeJob.RunningTasks[context.Id].SearchProgress.ContinuationToken);
            Assert.Empty(activeJob.TotalResourceCounts);
            Assert.Empty(activeJob.ProcessedResourceCounts);
            Assert.Empty(activeJob.SkippedResourceCounts);

            context.IsCompleted = true;
            context.SearchCount["Patient"] = 15;
            context.SkippedCount["Patient_customized"] = 2;
            context.ProcessedCount["Patient_customized"] = 13;
            await jobProgressUpdater.Produce(context);

            jobProgressUpdater.Complete();
            await updateTask;

            Assert.Empty(activeJob.RunningTasks);
            Assert.Equal(15, activeJob.TotalResourceCounts["Patient"]);
            Assert.Equal(10, activeJob.ProcessedResourceCounts["Patient"]);
            Assert.Equal(0, activeJob.SkippedResourceCounts["Patient"]);
            Assert.Equal(13, activeJob.ProcessedResourceCounts["Patient_customized"]);
            Assert.Equal(2, activeJob.SkippedResourceCounts["Patient_customized"]);

            var persistedJob = await containerClient.GetValue<Job>($"jobs/activeJobs/{activeJob.Id}.json");
            Assert.Equal(activeJob.ToString(), persistedJob.ToString());
            Assert.Empty(persistedJob.RunningTasks);
            Assert.Equal(15, persistedJob.TotalResourceCounts["Patient"]);
            Assert.Equal(10, persistedJob.ProcessedResourceCounts["Patient"]);
            Assert.Equal(0, persistedJob.SkippedResourceCounts["Patient"]);
        }

        private static JobProgressUpdater GetInMemoryJobProgressUpdater(Job job, IAzureBlobContainerClient containerClient)
        {
            var jobConfiguration = new JobConfiguration
            {
                ContainerName = "jobprogressupdater",
                StartTime = DateTimeOffset.MinValue,
                EndTime = DateTimeOffset.MaxValue,
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
