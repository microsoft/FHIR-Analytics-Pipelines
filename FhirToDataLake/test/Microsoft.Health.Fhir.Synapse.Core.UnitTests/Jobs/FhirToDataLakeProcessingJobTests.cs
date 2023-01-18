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
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
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
    public class FhirToDataLakeProcessingJobTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        private const string TestBlobEndpoint = "UseDevelopmentStorage=true";

        private static readonly List<TypeFilter> TestResourceTypeFilters = new List<TypeFilter> { new TypeFilter("Patient", null) };

        [Fact]
        public async Task GivenValidDataClient_WhenExecute_ThenTheDataShouldBeSavedToBlob()
        {
            string progressResult = null;
            Progress<string> progress = new Progress<string>(r =>
            {
                progressResult = r;
            });

            string containerName = Guid.NewGuid().ToString("N");

            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = "Patient",
            };

            var blobClient = new InMemoryBlobContainerClient();

            FhirToDataLakeProcessingJob job = GetFhirToDataLakeProcessingJob(1L, GetInputData(), TestDataProvider.GetDataFromFile(TestDataConstants.PatientBundleFile1), containerName, blobClient, filterConfiguration);

            string resultString = await job.ExecuteAsync(progress, CancellationToken.None);
            var result = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobResult>(resultString);

            Assert.NotNull(result);
            Assert.Equal(3, result.SearchCount["Patient"]);
            Assert.Equal(3, result.ProcessedCount["Patient"]);
            Assert.Equal(0, result.SkippedCount["Patient"]);
            Assert.Equal(52314, result.ProcessedDataSizeInTotal);
            Assert.Equal(3, result.ProcessedCountInTotal);

            await Task.Delay(TimeSpan.FromMilliseconds(100));
            var progressForContext = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobResult>(progressResult);
            Assert.NotNull(progressForContext);
            Assert.Equal(progressForContext.SearchCount["Patient"], result.SearchCount["Patient"]);
            Assert.Equal(progressForContext.ProcessedCount["Patient"], result.ProcessedCount["Patient"]);
            Assert.Equal(progressForContext.SkippedCount["Patient"], result.SkippedCount["Patient"]);
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
        public async Task GivenInvalidSearchBundle_WhenExecuteTask_ExceptionShouldBeThrown(string invalidBundle)
        {
            string progressResult = null;
            Progress<string> progress = new Progress<string>(r =>
            {
                progressResult = r;
            });

            string containerName = Guid.NewGuid().ToString("N");

            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = "Patient",
            };

            var blobClient = new InMemoryBlobContainerClient();

            FhirToDataLakeProcessingJob job = GetFhirToDataLakeProcessingJob(1L, GetInputData(), invalidBundle, containerName, blobClient, filterConfiguration);

            await Assert.ThrowsAsync<RetriableJobException>(() => job.ExecuteAsync(progress, CancellationToken.None));
        }

        private static FhirToDataLakeProcessingJob GetFhirToDataLakeProcessingJob(
            long jobId,
            FhirToDataLakeProcessingJobInputData inputData,
            string bundleResult,
            string containerName,
            IAzureBlobContainerClient blobClient,
            FilterConfiguration filterConfiguration = null)
        {
            return new FhirToDataLakeProcessingJob(
                jobId,
                inputData,
                GetMockFhirDataClient(bundleResult),
                GetDataWriter(containerName, blobClient),
                GetParquetDataProcessor(),
                GetSchemaManager(),
                GetGroupMemberExtractor(),
                GetFilterManager(filterConfiguration),
                new MetricsLogger(new NullLogger<MetricsLogger>()),
                _diagnosticLogger,
                new NullLogger<FhirToDataLakeProcessingJob>());
        }

        private static IApiDataClient GetMockFhirDataClient(string firstBundle)
        {
            var dataClient = Substitute.For<IApiDataClient>();

            // Get bundle from next link
            string nextBundle = TestDataProvider.GetDataFromFile(TestDataConstants.PatientBundleFile2);
            dataClient.SearchAsync(default).ReturnsForAnyArgs(firstBundle, nextBundle);
            return dataClient;
        }

        private static IDataWriter GetDataWriter(string containerName, IAzureBlobContainerClient blobClient)
        {
            var dataSourceOption = Options.Create(new DataSourceConfiguration());

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

        private static ParquetDataProcessor GetParquetDataProcessor()
        {
            IOptions<SchemaConfiguration> schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            var schemaManager = new ParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, _diagnosticLogger, NullLogger<ParquetSchemaManager>.Instance);
            IOptions<ArrowConfiguration> arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            var defaultConverter = new FhirDefaultSchemaConverter(schemaManager, _diagnosticLogger, NullLogger<FhirDefaultSchemaConverter>.Instance);
            var fhirConverter = new CustomSchemaConverter(TestUtils.GetMockAcrTemplateProvider(), schemaConfigurationOption, _diagnosticLogger, NullLogger<CustomSchemaConverter>.Instance);

            return new ParquetDataProcessor(
                schemaManager,
                arrowConfigurationOptions,
                TestUtils.TestDataSchemaConverterDelegate,
                _diagnosticLogger,
                NullLogger<ParquetDataProcessor>.Instance);
        }

        private static IParquetSchemaProvider ParquetSchemaProviderDelegate(string name)
        {
            var dataSourceOption = Options.Create(new DataSourceConfiguration());

            return new LocalDefaultSchemaProvider(dataSourceOption, _diagnosticLogger, NullLogger<LocalDefaultSchemaProvider>.Instance);
        }

        private static ISchemaManager<ParquetSchemaNode> GetSchemaManager()
        {
            IOptions<SchemaConfiguration> schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            return new ParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, _diagnosticLogger, NullLogger<ParquetSchemaManager>.Instance);
        }

        private static IFilterManager GetFilterManager(FilterConfiguration filterConfiguration)
        {
            filterConfiguration ??= new FilterConfiguration();
            var filterManager = Substitute.For<IFilterManager>();
            filterManager.GetTypeFiltersAsync(default).Returns(TestResourceTypeFilters);
            filterManager.GetFilterScopeAsync(default).Returns(filterConfiguration.FilterScope);
            filterManager.GetGroupIdAsync(default).Returns(filterConfiguration.GroupId);
            return filterManager;
        }

        private static IGroupMemberExtractor GetGroupMemberExtractor()
        {
            var groupMemberExtractor = Substitute.For<IGroupMemberExtractor>();
            HashSet<string> patients = new HashSet<string> { "patientId1", "patientId2" };
            groupMemberExtractor.GetGroupPatientsAsync(default, default, default, default).ReturnsForAnyArgs(patients);
            return groupMemberExtractor;
        }

        private FhirToDataLakeProcessingJobInputData GetInputData()
        {
            var inputData = new FhirToDataLakeProcessingJobInputData
            {
                JobVersion = JobVersionManager.CurrentJobVersion,
                JobType = JobType.Processing,
                TriggerSequenceId = 0L,
                ProcessingJobSequenceId = 0L,
                DataEndTime = DateTimeOffset.UtcNow,
                SplitParameters = new Dictionary<string, TimeRange> { { "Patient", new TimeRange() { DataEndTime = DateTimeOffset.UtcNow } } },
            };
            return inputData;
        }
    }
}
