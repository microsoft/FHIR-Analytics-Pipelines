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
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;
using Microsoft.Health.AnalyticsConnector.Common.Models.Jobs;
using Microsoft.Health.AnalyticsConnector.Core.DataProcessor;
using Microsoft.Health.AnalyticsConnector.Core.Jobs;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataWriter;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NSubstitute;
using NSubstitute.ReturnsExtensions;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.Jobs
{
    public class DicomToDatalakeAzureStorageJobFactoryTests
    {
        private const string TestWorkerName = "test-worker";
        private static string _jobVersionKey = nameof(FhirToDataLakeOrchestratorJobInputData.JobVersion);

        [Fact]
        public async Task GivenUnsupportedJobVersion_WhenCreateJob_ThenShouldReturnNull()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var jobFactory = new DicomToDatalakeAzureStorageJobFactory(
                queueClient,
                Substitute.For<IApiDataClient>(),
                Substitute.For<IDataWriter>(),
                Substitute.For<IColumnDataProcessor>(),
                Substitute.For<ISchemaManager<ParquetSchemaNode>>(),
                Options.Create(new JobConfiguration()),
                new MetricsLogger(new NullLogger<MetricsLogger>()),
                new DiagnosticLogger(),
                new NullLoggerFactory());

            // create job with unsupported job version
            var orchestratorDefinition = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                StartOffset = 0,
                EndOffset = 100,
            };

            var jobject = JObject.FromObject(orchestratorDefinition);

            jobject[_jobVersionKey] = "UnsupportedJobVersion";

            // enqueue job
            List<JobInfo> jobInfoList = (await queueClient.EnqueueAsync(
                (byte)QueueType.DicomToDataLake,
                new[] { JsonConvert.SerializeObject(jobject) },
                0,
                false,
                false,
                CancellationToken.None)).ToList();

            Assert.Single(jobInfoList);

            // dequeue job
            var jobInfo = await queueClient.DequeueAsync((byte)QueueType.DicomToDataLake, TestWorkerName, 0, CancellationToken.None);

            Assert.NotNull(jobInfo);
            var job = jobFactory.Create(jobInfo);

            // the job is null for unsupported job version
            Assert.Null(job);
        }

        [Fact]
        public async Task GivenJobFactoryReturnNull_WhenRunningJobHosting_ThenShouldSkipAndRevisualableInQueue()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>();

            var jobFactory = Substitute.For<IJobFactory>();
            jobFactory.Create(default).ReturnsNullForAnyArgs();

            // enqueue manually job
            var orchestratorDefinition = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = JobVersion.V1,
                StartOffset = 0,
                EndOffset = 100,
            };

            List<JobInfo> jobInfoList = (await queueClient.EnqueueAsync(
                (byte)QueueType.DicomToDataLake,
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
            Task task = jobHosting.StartAsync((byte)QueueType.DicomToDataLake, TestWorkerName, cancellationTokenSource);

            var jobInfo = await queueClient.DequeueAsync((byte)QueueType.DicomToDataLake, TestWorkerName, 1, CancellationToken.None);
            Assert.Null(jobInfo);
            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);

            // the job is re-visiable
            jobInfo = await queueClient.DequeueAsync((byte)QueueType.DicomToDataLake, TestWorkerName, 1, CancellationToken.None);

            Assert.NotNull(jobInfo);
            Assert.Equal(JobStatus.Running, jobInfo.Status);

            cancellationTokenSource.Cancel();
            await task;
        }
    }
}
