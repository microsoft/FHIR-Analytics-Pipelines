// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using NSubstitute;
using NSubstitute.ReturnsExtensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class JobHostingTests
    {
        private const string TestWorkerName = "test-worker";

        [Fact]
        public async Task GivenUnsupportedJobVersion_WhenCreateJob_ThenShouldSkipAndRevisualableInQueue()
        {
            var queueClient = new MockQueueClient<FhirToDataLakeAzureStorageJobInfo>();

            var jobFactory = Substitute.For<IJobFactory>();
            jobFactory.Create(default).ReturnsNullForAnyArgs();

            // enqueue manually job
            var orchestratorDefinition = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = SupportedJobVersion.V1,
                DataStartTime = null,
                DataEndTime = DateTime.UtcNow,
            };

            List<JobInfo> jobInfoList = (await queueClient.EnqueueAsync(
                (byte)QueueType.FhirToDataLake,
                new[] { JsonConvert.SerializeObject(orchestratorDefinition) },
                0,
                false,
                false,
                CancellationToken.None)).ToList();

            // the job is enqueued
            Assert.Single(jobInfoList);

            var jobHosting = new JobHosting(queueClient, jobFactory, new NullLogger<JobHosting>());

            using var cancellationTokenSource = new CancellationTokenSource();

            // queue message visibilityTimeout
            jobHosting.JobHeartbeatTimeoutThresholdInSeconds = 1;
            Task task = jobHosting.StartAsync((byte)QueueType.FhirToDataLake, TestWorkerName, cancellationTokenSource);

            var jobInfo = await queueClient.DequeueAsync((byte)QueueType.FhirToDataLake, TestWorkerName, 1, CancellationToken.None);
            Assert.Null(jobInfo);
            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            // the job is re-visiable
            jobInfo = await queueClient.DequeueAsync((byte)QueueType.FhirToDataLake, TestWorkerName, 1, CancellationToken.None);

            Assert.NotNull(jobInfo);
            Assert.Equal(JobStatus.Running, jobInfo.Status);
        }
    }
}
