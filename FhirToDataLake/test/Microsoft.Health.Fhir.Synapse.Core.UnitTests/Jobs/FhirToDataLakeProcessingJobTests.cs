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
        private const string TestBlobEndpoint = "UseDevelopmentStorage=true";

        private static readonly List<TypeFilter> TestResourceTypeFilters =
            new () { new TypeFilter("Patient", null) };

        [Fact]
        public async Task GivenValidDataClient_WhenExecute_ThenTheDataShouldBeSavedToBlob()
        {
            string progressResult = null;
            var progress = new Progress<string>((r) =>
            {
                progressResult = r;
            });

            var containerName = Guid.NewGuid().ToString("N");

            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = "Patient",
            };

            var blobClient = new InMemoryBlobContainerClient();

            var job = GetFhirToDataLakeProcessingJob(1L, GetInputData(), TestDataProvider.GetBundleFromFile(TestDataConstants.PatientBundleFile1), containerName, blobClient, filterConfiguration);

            var resultString = await job.ExecuteAsync(progress, CancellationToken.None);
            var result = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobResult>(resultString);

            Assert.NotNull(result);
            Assert.Equal(3, result.SearchCount["Patient"]);
            Assert.Equal(3, result.ProcessedCount["Patient"]);
            Assert.Equal(0, result.SkippedCount["Patient"]);

            await Task.Delay(TimeSpan.FromMilliseconds(100));
            var progressForContext = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobResult>(progressResult);
            Assert.NotNull(progressForContext);
            Assert.Equal(progressForContext.SearchCount["Patient"], result.SearchCount["Patient"]);
            Assert.Equal(progressForContext.ProcessedCount["Patient"], result.ProcessedCount["Patient"]);
            Assert.Equal(progressForContext.SkippedCount["Patient"], result.SkippedCount["Patient"]);

            // verify blob data;
            var blobs = await blobClient.ListBlobsAsync("staging");
            Assert.Single(blobs);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("{entry: [{resource: {}}]}")]
        public async Task GivenInvalidSearchBundle_WhenExecuteTask_ExceptionShouldBeThrown(string invalidBundle)
        {
            string progressResult = null;
            var progress = new Progress<string>((r) =>
            {
                progressResult = r;
            });

            var containerName = Guid.NewGuid().ToString("N");

            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = "Patient",
            };

            var blobClient = new InMemoryBlobContainerClient();

            var job = GetFhirToDataLakeProcessingJob(1L, GetInputData(), invalidBundle, containerName, blobClient, filterConfiguration);

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
                new NullLogger<FhirToDataLakeProcessingJob>());
        }

        private static IFhirDataClient GetMockFhirDataClient(string firstBundle)
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            // Get bundle from next link
            var nextBundle = TestDataProvider.GetBundleFromFile(TestDataConstants.PatientBundleFile2);
            dataClient.SearchAsync(default).ReturnsForAnyArgs(firstBundle, nextBundle);
            return dataClient;
        }

        private static IFhirDataWriter GetDataWriter(string containerName, IAzureBlobContainerClient blobClient)
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
            return new AzureBlobDataWriter(mockFactory, dataSink, new NullLogger<AzureBlobDataWriter>());
        }

        private static ParquetDataProcessor GetParquetDataProcessor()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            var fhirSchemaManager = new FhirParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, NullLogger<FhirParquetSchemaManager>.Instance);
            var arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            var defaultConverter = new DefaultSchemaConverter(fhirSchemaManager, NullLogger<DefaultSchemaConverter>.Instance);
            var fhirConverter = new CustomSchemaConverter(TestUtils.GetMockAcrTemplateProvider(), schemaConfigurationOption, NullLogger<CustomSchemaConverter>.Instance);

            return new ParquetDataProcessor(
                fhirSchemaManager,
                arrowConfigurationOptions,
                TestUtils.TestDataSchemaConverterDelegate,
                NullLogger<ParquetDataProcessor>.Instance);
        }

        private static IParquetSchemaProvider ParquetSchemaProviderDelegate(string name)
        {
            return new LocalDefaultSchemaProvider(Options.Create(new FhirServerConfiguration()), NullLogger<LocalDefaultSchemaProvider>.Instance);
        }

        private static IFhirSchemaManager<FhirParquetSchemaNode> GetFhirSchemaManager()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            return new FhirParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, NullLogger<FhirParquetSchemaManager>.Instance);
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
            var patients = new HashSet<string> { "patientId1", "patientId2" };
            groupMemberExtractor.GetGroupPatientsAsync(default, default, default, default).ReturnsForAnyArgs(patients);
            return groupMemberExtractor;
        }

        private FhirToDataLakeProcessingJobInputData GetInputData()
        {
            var inputData = new FhirToDataLakeProcessingJobInputData
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
