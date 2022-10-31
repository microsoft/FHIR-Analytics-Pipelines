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

        private static readonly List<TypeFilter> TestResourceTypeFilters =
            new () { new TypeFilter("Patient", null) };

        [Fact]
        public async Task GivenValidDataClient_WhenExecute_ThenTheDataShouldBeSavedToBlob()
        {
            string progressResult = null;
            Progress<string> progress = new Progress<string>((r) =>
            {
                progressResult = r;
            });

            string containerName = Guid.NewGuid().ToString("N");

            FilterConfiguration filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = "Patient",
            };

            InMemoryBlobContainerClient blobClient = new InMemoryBlobContainerClient();

            FhirToDataLakeProcessingJob job = GetFhirToDataLakeProcessingJob(1L, GetInputData(), TestDataProvider.GetBundleFromFile(TestDataConstants.PatientBundleFile1), containerName, blobClient, filterConfiguration);

            string resultString = await job.ExecuteAsync(progress, CancellationToken.None);
            FhirToDataLakeProcessingJobResult result = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobResult>(resultString);

            Assert.NotNull(result);
            Assert.Equal(3, result.SearchCount["Patient"]);
            Assert.Equal(3, result.ProcessedCount["Patient"]);
            Assert.Equal(0, result.SkippedCount["Patient"]);
            Assert.Equal(52314, result.ProcessedDataSizeInTotal);
            Assert.Equal(3, result.ProcessedCountInTotal);

            await Task.Delay(TimeSpan.FromMilliseconds(100));
            FhirToDataLakeProcessingJobResult progressForContext = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobResult>(progressResult);
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
            Progress<string> progress = new Progress<string>((r) =>
            {
                progressResult = r;
            });

            string containerName = Guid.NewGuid().ToString("N");

            FilterConfiguration filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = "Patient",
            };

            InMemoryBlobContainerClient blobClient = new InMemoryBlobContainerClient();

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
                GetFhirSchemaManager(),
                GetGroupMemberExtractor(),
                GetFilterManager(filterConfiguration),
                _diagnosticLogger,
                new NullLogger<FhirToDataLakeProcessingJob>());
        }

        private static IFhirDataClient GetMockFhirDataClient(string firstBundle)
        {
            IFhirDataClient dataClient = Substitute.For<IFhirDataClient>();

            // Get bundle from next link
            string nextBundle = TestDataProvider.GetBundleFromFile(TestDataConstants.PatientBundleFile2);
            dataClient.SearchAsync(default).ReturnsForAnyArgs(firstBundle, nextBundle);
            return dataClient;
        }

        private static IFhirDataWriter GetDataWriter(string containerName, IAzureBlobContainerClient blobClient)
        {
            IAzureBlobContainerClientFactory mockFactory = Substitute.For<IAzureBlobContainerClientFactory>();
            mockFactory.Create(Arg.Any<string>(), Arg.Any<string>()).ReturnsForAnyArgs(blobClient);

            DataLakeStoreConfiguration storageConfig = new DataLakeStoreConfiguration
            {
                StorageUrl = TestBlobEndpoint,
            };
            JobConfiguration jobConfig = new JobConfiguration
            {
                ContainerName = containerName,
            };

            AzureBlobDataSink dataSink = new AzureBlobDataSink(Options.Create(storageConfig), Options.Create(jobConfig));
            return new AzureBlobDataWriter(mockFactory, dataSink, new NullLogger<AzureBlobDataWriter>());
        }

        private static ParquetDataProcessor GetParquetDataProcessor()
        {
            IOptions<SchemaConfiguration> schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            FhirParquetSchemaManager fhirSchemaManager = new FhirParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, NullLogger<FhirParquetSchemaManager>.Instance);
            IOptions<ArrowConfiguration> arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            DefaultSchemaConverter defaultConverter = new DefaultSchemaConverter(fhirSchemaManager, _diagnosticLogger, NullLogger<DefaultSchemaConverter>.Instance);
            CustomSchemaConverter fhirConverter = new CustomSchemaConverter(TestUtils.GetMockAcrTemplateProvider(), schemaConfigurationOption, _diagnosticLogger, NullLogger<CustomSchemaConverter>.Instance);

            return new ParquetDataProcessor(
                fhirSchemaManager,
                arrowConfigurationOptions,
                TestUtils.TestDataSchemaConverterDelegate,
                _diagnosticLogger,
                NullLogger<ParquetDataProcessor>.Instance);
        }

        private static IParquetSchemaProvider ParquetSchemaProviderDelegate(string name)
        {
            return new LocalDefaultSchemaProvider(Options.Create(new FhirServerConfiguration()), _diagnosticLogger, NullLogger<LocalDefaultSchemaProvider>.Instance);
        }

        private static IFhirSchemaManager<FhirParquetSchemaNode> GetFhirSchemaManager()
        {
            IOptions<SchemaConfiguration> schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            return new FhirParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, NullLogger<FhirParquetSchemaManager>.Instance);
        }

        private static IFilterManager GetFilterManager(FilterConfiguration filterConfiguration)
        {
            filterConfiguration ??= new FilterConfiguration();
            IFilterManager filterManager = Substitute.For<IFilterManager>();
            filterManager.GetTypeFiltersAsync(default).Returns(TestResourceTypeFilters);
            filterManager.GetFilterScopeAsync(default).Returns(filterConfiguration.FilterScope);
            filterManager.GetGroupIdAsync(default).Returns(filterConfiguration.GroupId);
            return filterManager;
        }

        private static IGroupMemberExtractor GetGroupMemberExtractor()
        {
            IGroupMemberExtractor groupMemberExtractor = Substitute.For<IGroupMemberExtractor>();
            HashSet<string> patients = new HashSet<string> { "patientId1", "patientId2" };
            groupMemberExtractor.GetGroupPatientsAsync(default, default, default, default).ReturnsForAnyArgs(patients);
            return groupMemberExtractor;
        }

        private FhirToDataLakeProcessingJobInputData GetInputData()
        {
            FhirToDataLakeProcessingJobInputData inputData = new FhirToDataLakeProcessingJobInputData
            {
                JobType = JobType.Processing,
                TriggerSequenceId = 0L,
                ProcessingJobSequenceId = 0L,
                DataEndTime = DateTimeOffset.UtcNow,
            };
            return inputData;
        }
    }
}
