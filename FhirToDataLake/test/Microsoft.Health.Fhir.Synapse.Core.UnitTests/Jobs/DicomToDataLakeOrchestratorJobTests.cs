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
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class DicomToDataLakeOrchestratorJobTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private const string TestBlobEndpoint = "UseDevelopmentStorage=true";
        private const long TBValue = 1024L * 1024L * 1024L * 1024L;
        private static readonly long JobChangeFeedLimit = JobConfigurationConstants.DicomJobChangeFeedLimit;
        private static readonly long SearchChangeFeedLimit = JobConfigurationConstants.DicomSearchChangeFeedLimit;
        private static readonly long TestStartOffset = 0;
        private static readonly long TestEndOffset = (JobChangeFeedLimit * 10) + 10;

        [Fact]
        public async Task GivenANewOrchestratorJob_WhenProcessingInputFilesMoreThanConcurrentCount_ThenJobShouldBeCompleted()
        {
            // offset from 0 to {TestEndOffset}, every {JobChangeFeedLimit} changefeeds one job, so there are {TestEndOffset/JobChangeFeedLimit} jobs.
            await VerifyCommonOrchestratorJobAsync(11, 2);
        }

        [Fact]
        public async Task GivenANewOrchestratorJob_WhenProcessingInputFilesEqualsConcurrentCount_ThenJobShouldBeCompleted()
        {
            await VerifyCommonOrchestratorJobAsync(11, 11);
        }

        [Fact]
        public async Task GivenANewOrchestratorJob_WhenProcessingInputFilesLessThanConcurrentCount_ThenJobShouldBeCompleted()
        {
            await VerifyCommonOrchestratorJobAsync(11, 12);
        }

        [Fact]
        public async Task GivenAResumedOrchestratorJob_WhenExecute_ThenJobShouldBeCompleted()
        {
            await VerifyCommonOrchestratorJobAsync(11, 2, 1);
        }

        [Fact]
        public async Task GivenAResumedOrchestratorJob_WhenExecuteSomeJobCompleted_ThenJobShouldBeCompleted()
        {
            await VerifyCommonOrchestratorJobAsync(11, 2, 5, 3);
        }

        private static async Task<DicomToDataLakeOrchestratorJobResult> VerifyCommonOrchestratorJobAsync(
            int inputFileCount,
            int concurrentCount,
            int resumeFrom = -1,
            int completedCount = 0)
        {
            string progressResult = null;
            Progress<string> progress = new Progress<string>(r =>
            {
                progressResult = r;
            });

            string containerName = Guid.NewGuid().ToString("N");

            var blobClient = new InMemoryBlobContainerClient();

            DicomToDataLakeOrchestratorJobInputData inputData = GetInputData();
            var queueClient = GetQueueClient();
            List<JobInfo> jobInfoList = (await queueClient.EnqueueAsync(
                (byte)QueueType.DicomToDataLake,
                new[] { JsonConvert.SerializeObject(inputData) },
                inputData.TriggerSequenceId,
                false,
                false,
                CancellationToken.None)).ToList();
            Assert.Single(jobInfoList);
            JobInfo orchestratorJobInfo = jobInfoList.First();

            var orchestratorJobResult = new DicomToDataLakeOrchestratorJobResult();
            bool resumeMode = resumeFrom >= 0;
            for (int i = 0; i < inputFileCount; ++i)
            {
                if (resumeMode)
                {
                    if (i <= resumeFrom)
                    {
                        var processingInput = new DicomToDataLakeProcessingJobInputData
                        {
                            JobType = JobType.Processing,
                            ProcessingJobSequenceId = i,
                            TriggerSequenceId = inputData.TriggerSequenceId,
                            StartOffset = TestStartOffset + (i * JobChangeFeedLimit),
                            EndOffset = TestStartOffset + ((i + 1) * JobChangeFeedLimit),
                        };

                        JobInfo jobInfo = (await queueClient.EnqueueAsync(0, new[] { JsonConvert.SerializeObject(processingInput) }, 1, false, false, CancellationToken.None)).First();

                        var processingResult = new DicomToDataLakeProcessingJobResult
                        {
                            SearchCount = new Dictionary<string, int> { { "Dicom", 1 } },
                            ProcessedCount = new Dictionary<string, int> { { "Dicom", 1 } },
                            SkippedCount = new Dictionary<string, int> { { "Dicom", 0 } },
                            ProcessedCountInTotal = 1,
                            ProcessedDataSizeInTotal = 1000L * TBValue,
                        };

                        jobInfo.Result = JsonConvert.SerializeObject(processingResult);
                        if (i < completedCount)
                        {
                            jobInfo.Status = JobStatus.Completed;
                            orchestratorJobResult.TotalResourceCounts.ConcatDictionaryCount(processingResult.SearchCount);
                            orchestratorJobResult.ProcessedResourceCounts.ConcatDictionaryCount(processingResult.ProcessedCount);
                            orchestratorJobResult.ProcessedCountInTotal += processingResult.ProcessedCountInTotal;
                            orchestratorJobResult.ProcessedDataSizeInTotal += processingResult.ProcessedDataSizeInTotal;
                        }
                        else
                        {
                            jobInfo.Status = JobStatus.Running;
                            orchestratorJobResult.RunningJobIds.Add(jobInfo.Id);
                        }

                        orchestratorJobResult.CreatedJobCount += 1;
                        orchestratorJobResult.NextOffset = (i + 1) * JobChangeFeedLimit;
                    }
                }
            }

            for (int i = 0; i < inputFileCount; ++i)
            {
                if (i < completedCount)
                {
                    await CreateBlobForProcessingJob(orchestratorJobInfo.Id + i + 1, (i + 1) * SearchChangeFeedLimit, true, blobClient);
                }
                else
                {
                    await CreateBlobForProcessingJob(orchestratorJobInfo.Id + i + 1, (i + 1) * SearchChangeFeedLimit, false, blobClient);
                }
            }

            var job = new DicomToDataLakeOrchestratorJob(
                orchestratorJobInfo,
                inputData,
                orchestratorJobResult,
                GetDataWriter(containerName, blobClient),
                queueClient,
                concurrentCount,
                new MetricsLogger(new NullLogger<MetricsLogger>()),
                _diagnosticLogger,
                new NullLogger<DicomToDataLakeOrchestratorJob>());

            string resultString = await job.ExecuteAsync(progress, CancellationToken.None);
            var result = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobResult>(resultString);

            Assert.NotNull(result);
            Assert.Equal(inputFileCount, result.CreatedJobCount);
            Assert.Empty(result.RunningJobIds);
            Assert.Equal(inputFileCount, result.TotalResourceCounts["Dicom"]);
            Assert.Equal(inputFileCount, result.ProcessedResourceCounts["Dicom"]);
            Assert.Equal(0, result.SkippedResourceCounts["Dicom"]);
            Assert.Equal(inputFileCount, result.ProcessedCountInTotal);
            Assert.Equal(inputFileCount * 1000L * TBValue, result.ProcessedDataSizeInTotal);

            await Task.Delay(TimeSpan.FromMilliseconds(100));
            var progressForContext = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobResult>(progressResult);
            Assert.NotNull(progressForContext);
            Assert.Equal(inputFileCount, progressForContext.CreatedJobCount);
            Assert.Empty(progressForContext.RunningJobIds);
            Assert.Equal(progressForContext.TotalResourceCounts["Dicom"], result.TotalResourceCounts["Dicom"]);
            Assert.Equal(progressForContext.ProcessedResourceCounts["Dicom"], result.ProcessedResourceCounts["Dicom"]);
            Assert.Equal(progressForContext.SkippedResourceCounts["Dicom"], result.SkippedResourceCounts["Dicom"]);
            Assert.Equal(progressForContext.ProcessedDataSizeInTotal, result.ProcessedDataSizeInTotal);
            Assert.Equal(progressForContext.ProcessedCountInTotal, result.ProcessedCountInTotal);

            Assert.Equal(inputFileCount, queueClient.JobInfos.Count - 1);

            // verify blob data;
            IEnumerable<string> blobs = await blobClient.ListBlobsAsync("staging");
            Assert.Empty(blobs);

            blobs = await blobClient.ListBlobsAsync("result");
            Assert.Equal(inputFileCount, blobs.Count());

            return result;
        }

        private static MockQueueClient<DicomToDataLakeAzureStorageJobInfo> GetQueueClient()
        {
            var queueClient = new MockQueueClient<DicomToDataLakeAzureStorageJobInfo>
            {
                GetJobByIdFunc = (queueClient, id,  _) =>
                {
                    JobInfo jobInfo = queueClient.JobInfos.First(t => t.Id == id);

                    if (jobInfo == null)
                    {
                        return null;
                    }

                    if (jobInfo.Status == JobStatus.Completed)
                    {
                        return jobInfo;
                    }

                    var processingResult = new DicomToDataLakeProcessingJobResult
                    {
                        SearchCount = new Dictionary<string, int> { { "Dicom", 1 } },
                        ProcessedCount = new Dictionary<string, int> { { "Dicom", 1 } },
                        SkippedCount = new Dictionary<string, int> { { "Dicom", 0 } },
                        ProcessedCountInTotal = 1,
                        ProcessedDataSizeInTotal = 1000L * TBValue,
                    };

                    jobInfo.Result = JsonConvert.SerializeObject(processingResult);
                    jobInfo.Status = JobStatus.Completed;
                    return jobInfo;
                },
            };

            return queueClient;
        }

        private static DicomToDataLakeOrchestratorJobInputData GetInputData()
        {
            return new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 0L,
                StartOffset = TestStartOffset,
                EndOffset = TestEndOffset,
            };
        }

        private static IDataWriter GetDataWriter(string containerName, IAzureBlobContainerClient blobClient)
        {
            var dataSourceOption = Options.Create(new DataSourceConfiguration { Type = Common.DataSourceType.DICOM });

            var mockFactory = Substitute.For<IAzureBlobContainerClientFactory>();
            mockFactory.Create(Arg.Any<string>(), Arg.Any<string>()).ReturnsForAnyArgs(blobClient);

            var storageConfig = new DataLakeStoreConfiguration
            {
                StorageUrl = TestBlobEndpoint,
            };
            var jobConfig = new JobConfiguration
            {
                ContainerName = containerName,
            };

            var dataSink = new AzureBlobDataSink(Options.Create(storageConfig), Options.Create(jobConfig));
            return new AzureBlobDataWriter(dataSourceOption, mockFactory, dataSink, new NullLogger<AzureBlobDataWriter>());
        }

        private static async Task CreateBlobForProcessingJob(long jobId, long endOffset, bool isCompleted, IAzureBlobContainerClient blobClient)
        {
            var stream = new MemoryStream();
            byte[] bytes = { 1, 2, 3, 4, 5, 6 };
            await stream.WriteAsync(bytes, 0, bytes.Length);
            stream.Position = 0;
            int partId = 0;
            string blobName = isCompleted
                ? $"result/Dicom/{endOffset}/{jobId:d20}/Dicom_{partId:d10}.parquet"
                : $"staging/{jobId:d20}/Dicom/{endOffset}/Dicom_{partId:d10}.parquet";

            await blobClient.CreateBlobAsync(blobName, stream, CancellationToken.None);
        }
    }
}