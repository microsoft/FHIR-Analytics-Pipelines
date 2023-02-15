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
using Microsoft.Health.AnalyticsConnector.Common;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;
using Microsoft.Health.AnalyticsConnector.Core.DataProcessor;
using Microsoft.Health.AnalyticsConnector.Core.Jobs;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataClient.UnitTests;
using Microsoft.Health.AnalyticsConnector.DataWriter;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.Jobs
{
    public class DicomToDataLakeProcessingJobTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        private const string TestBlobEndpoint = "UseDevelopmentStorage=true";
        private static IOptions<DataSourceConfiguration> _dataSourceOption = Options.Create(new DataSourceConfiguration { Type = DataSourceType.DICOM });

        [Fact]
        public async Task GivenValidDataClient_WhenExecute_ThenTheDataShouldBeSavedToBlob()
        {
            string progressResult = null;
            Progress<string> progress = new Progress<string>(r =>
            {
                progressResult = r;
            });

            string containerName = Guid.NewGuid().ToString("N");

            var blobClient = new InMemoryBlobContainerClient();

            DicomToDataLakeProcessingJob job = GetDicomToDataLakeProcessingJob(1L, GetInputData(), TestDataProvider.GetDataFromFile(TestDataConstants.ChangeFeedsFile), containerName, blobClient);

            string resultString = await job.ExecuteAsync(progress, CancellationToken.None);
            var result = JsonConvert.DeserializeObject<DicomToDataLakeProcessingJobResult>(resultString);

            Assert.NotNull(result);
            Assert.Equal(3, result.SearchCount["Dicom"]);
            Assert.Equal(3, result.ProcessedCount["Dicom"]);
            Assert.Equal(0, result.SkippedCount["Dicom"]);
            Assert.Equal(911130, result.ProcessedDataSizeInTotal);
            Assert.Equal(3, result.ProcessedCountInTotal);

            await Task.Delay(TimeSpan.FromMilliseconds(100));
            var progressForContext = JsonConvert.DeserializeObject<DicomToDataLakeProcessingJobResult>(progressResult);
            Assert.NotNull(progressForContext);
            Assert.Equal(progressForContext.SearchCount["Dicom"], result.SearchCount["Dicom"]);
            Assert.Equal(progressForContext.ProcessedCount["Dicom"], result.ProcessedCount["Dicom"]);
            Assert.Equal(progressForContext.SkippedCount["Dicom"], result.SkippedCount["Dicom"]);
            Assert.Equal(progressForContext.ProcessedDataSizeInTotal, result.ProcessedDataSizeInTotal);
            Assert.Equal(progressForContext.ProcessedCountInTotal, result.ProcessedCountInTotal);

            // verify blob data;
            IEnumerable<string> blobs = await blobClient.ListBlobsAsync("staging");
            Assert.Single(blobs);
        }

        [Fact]
        public async Task GivenChangeFeedAllReplacedSearchResult_WhenExecuteTask_EmptyResultShouldBeThrown()
        {
            string changeFeedAllReplacedSearchResult = "[{\"action\":\"Create\", \"state\":\"Replaced\"},{\"action\":\"Delete\", \"state\":\"Replaced\"}]";
            string progressResult = null;
            Progress<string> progress = new Progress<string>(r =>
            {
                progressResult = r;
            });

            string containerName = Guid.NewGuid().ToString("N");

            var blobClient = new InMemoryBlobContainerClient();
            DicomToDataLakeProcessingJob job = GetDicomToDataLakeProcessingJob(1L, GetInputData(), changeFeedAllReplacedSearchResult, containerName, blobClient);
            string resultString = await job.ExecuteAsync(progress, CancellationToken.None);
            var result = JsonConvert.DeserializeObject<DicomToDataLakeProcessingJobResult>(resultString);

            Assert.NotNull(result);
            Assert.Equal(0, result.SearchCount["Dicom"]);
            Assert.Equal(0, result.ProcessedCount["Dicom"]);
            Assert.Equal(0, result.SkippedCount["Dicom"]);
            Assert.Equal(0, result.ProcessedDataSizeInTotal);
            Assert.Equal(0, result.ProcessedCountInTotal);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("{entry: [{resource: {}}]}")]
        public async Task GivenInvalidSearchResult_WhenExecuteTask_ExceptionShouldBeThrown(string invalidSearchResult)
        {
            string progressResult = null;
            Progress<string> progress = new Progress<string>(r =>
            {
                progressResult = r;
            });

            string containerName = Guid.NewGuid().ToString("N");

            var blobClient = new InMemoryBlobContainerClient();
            var metricsLogger = new MockMetricsLogger(null);
            DicomToDataLakeProcessingJob job = GetDicomToDataLakeProcessingJob(1L, GetInputData(), invalidSearchResult, containerName, blobClient, metricsLogger);

            await Assert.ThrowsAsync<RetriableJobException>(() => job.ExecuteAsync(progress, CancellationToken.None));
            Assert.Equal(1, metricsLogger.MetricsDic["TotalError"]);
            Assert.Equal("RunJob", metricsLogger.ErrorOperationType);
        }

        private static DicomToDataLakeProcessingJob GetDicomToDataLakeProcessingJob(
            long jobId,
            DicomToDataLakeProcessingJobInputData inputData,
            string searchResult,
            string containerName,
            IAzureBlobContainerClient blobClient,
            IMetricsLogger metricsLogger = null)
        {
            return new DicomToDataLakeProcessingJob(
                jobId,
                inputData,
                GetMockDicomDataClient(searchResult),
                GetDataWriter(containerName, blobClient),
                GetParquetDataProcessor(),
                GetSchemaManager(),
                metricsLogger ?? new MockMetricsLogger(null),
                _diagnosticLogger,
                new NullLogger<DicomToDataLakeProcessingJob>());
        }

        private static IApiDataClient GetMockDicomDataClient(string firstResult)
        {
            var dataClient = Substitute.For<IApiDataClient>();

            // Get result from next link
            string nextResult = TestDataProvider.GetDataFromFile(TestDataConstants.ChangeFeedsFile);
            dataClient.SearchAsync(default).ReturnsForAnyArgs(firstResult, nextResult);
            return dataClient;
        }

        private static IDataWriter GetDataWriter(string containerName, IAzureBlobContainerClient blobClient)
        {
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
            return new AzureBlobDataWriter(_dataSourceOption, mockFactory, dataSink, new NullLogger<AzureBlobDataWriter>());
        }

        private static ParquetDataProcessor GetParquetDataProcessor()
        {
            return new ParquetDataProcessor(
                GetSchemaManager(),
                TestUtils.TestDicomDataSchemaConverterDelegate,
                _diagnosticLogger,
                NullLogger<ParquetDataProcessor>.Instance);
        }

        private static ISchemaManager<ParquetSchemaNode> GetSchemaManager()
        {
            IOptions<SchemaConfiguration> schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            return new ParquetSchemaManager(schemaConfigurationOption, TestUtils.TestDicomParquetSchemaProviderDelegate, _diagnosticLogger, NullLogger<ParquetSchemaManager>.Instance);
        }

        private static DicomToDataLakeProcessingJobInputData GetInputData()
        {
            var inputData = new DicomToDataLakeProcessingJobInputData
            {
                JobType = JobType.Processing,
                TriggerSequenceId = 0L,
                ProcessingJobSequenceId = 0L,
                StartOffset = 0,
                EndOffset = 10,
            };
            return inputData;
        }
    }
}
