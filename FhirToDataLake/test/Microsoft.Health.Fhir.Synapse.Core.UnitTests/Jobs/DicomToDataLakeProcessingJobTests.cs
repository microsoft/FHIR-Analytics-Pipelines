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
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
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
            Assert.Equal(3, result.SearchCount["dicom"]);
            Assert.Equal(3, result.ProcessedCount["dicom"]);
            Assert.Equal(0, result.SkippedCount["dicom"]);
            Assert.Equal(911016, result.ProcessedDataSizeInTotal);
            Assert.Equal(3, result.ProcessedCountInTotal);

            await Task.Delay(TimeSpan.FromMilliseconds(100));
            var progressForContext = JsonConvert.DeserializeObject<DicomToDataLakeProcessingJobResult>(progressResult);
            Assert.NotNull(progressForContext);
            Assert.Equal(progressForContext.SearchCount["dicom"], result.SearchCount["dicom"]);
            Assert.Equal(progressForContext.ProcessedCount["dicom"], result.ProcessedCount["dicom"]);
            Assert.Equal(progressForContext.SkippedCount["dicom"], result.SkippedCount["dicom"]);
            Assert.Equal(progressForContext.ProcessedDataSizeInTotal, result.ProcessedDataSizeInTotal);
            Assert.Equal(progressForContext.ProcessedCountInTotal, result.ProcessedCountInTotal);

            // verify blob data;
            IEnumerable<string> blobs = await blobClient.ListBlobsAsync("staging");
            Assert.Single(blobs);
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

            DicomToDataLakeProcessingJob job = GetDicomToDataLakeProcessingJob(1L, GetInputData(), invalidSearchResult, containerName, blobClient);

            await Assert.ThrowsAsync<RetriableJobException>(() => job.ExecuteAsync(progress, CancellationToken.None));
        }

        private static DicomToDataLakeProcessingJob GetDicomToDataLakeProcessingJob(
            long jobId,
            DicomToDataLakeProcessingJobInputData inputData,
            string searchResult,
            string containerName,
            IAzureBlobContainerClient blobClient)
        {
            return new DicomToDataLakeProcessingJob(
                jobId,
                inputData,
                GetMockDicomDataClient(searchResult),
                GetDataWriter(containerName, blobClient),
                GetParquetDataProcessor(),
                GetSchemaManager(),
                new MetricsLogger(new NullLogger<MetricsLogger>()),
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
            IOptions<SchemaConfiguration> schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            var schemaManager = new ParquetSchemaManager(schemaConfigurationOption, TestUtils.TestDicomParquetSchemaProviderDelegate, _diagnosticLogger, NullLogger<ParquetSchemaManager>.Instance);
            IOptions<ArrowConfiguration> arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            return new ParquetDataProcessor(
                schemaManager,
                arrowConfigurationOptions,
                TestUtils.TestDicomDataSchemaConverterDelegate,
                _diagnosticLogger,
                NullLogger<ParquetDataProcessor>.Instance);
        }

        private static ISchemaManager<ParquetSchemaNode> GetSchemaManager()
        {
            IOptions<SchemaConfiguration> schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            return new ParquetSchemaManager(schemaConfigurationOption, TestUtils.TestDicomParquetSchemaProviderDelegate, _diagnosticLogger, NullLogger<ParquetSchemaManager>.Instance);
        }

        private DicomToDataLakeProcessingJobInputData GetInputData()
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
