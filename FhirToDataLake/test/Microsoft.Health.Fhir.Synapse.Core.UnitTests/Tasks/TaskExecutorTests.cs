// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Tasks
{
    public class TaskExecutorTests
    {
        private string TestBlobEndpoint = "UseDevelopmentStorage=true";

        [Fact]
        public async Task GivenValidDataClient_WhenExecuteTask_DataShouldBeSavedToBlob()
        {
            var containerName = Guid.NewGuid().ToString("N");
            var taskExecutor = GetTaskExecutor(TestDataProvider.GetBundleFromFile(TestDataConstants.PatientBundleFile1), containerName);

            var typeFilters = new List<TypeFilter> { new ("Patient", null) };
            var filterInfo = new FilterInfo(FilterScope.System, null, DateTimeOffset.MinValue, typeFilters, null);

            // Create an active job.
            var activeJob = Job.Create(
                containerName,
                JobStatus.Running,
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                filterInfo);

            var taskContext = TaskContext.CreateFromJob(activeJob, typeFilters);

            activeJob.RunningTasks[taskContext.Id] = taskContext;

            var jobUpdater = GetJobUpdater(activeJob);
            var taskResult = await taskExecutor.ExecuteAsync(taskContext, jobUpdater);

            // verify task result;
            Assert.True(taskResult.IsCompleted);
            Assert.Equal(3, taskResult.SearchCount["Patient"]);
            Assert.Equal(3, taskResult.ProcessedCount["Patient"]);
            Assert.Equal(0, taskResult.SkippedCount["Patient"]);

            jobUpdater.Complete();
            await jobUpdater.Consume();

            // verify blob data;
            var blobClient = new BlobContainerClient(TestBlobEndpoint, containerName);
            var blobPages = blobClient.GetBlobs(prefix: "staging").AsPages();
            Assert.Single(blobPages.First().Values);

            // verify job data
            var jobBlob = blobClient.GetBlobClient($"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            using var stream = new MemoryStream();
            jobBlob.DownloadTo(stream);
            stream.Position = 0;
            using var streamReader = new StreamReader(stream);
            var jobContent = streamReader.ReadToEnd();
            var job = JsonConvert.DeserializeObject<Job>(jobContent);
            Assert.Equal(activeJob.Id, job.Id);

            Assert.Empty(job.RunningTasks);

            Assert.Equal(3, job.ProcessedResourceCounts["Patient"]);
            Assert.Equal(3, job.TotalResourceCounts["Patient"]);
            Assert.Equal(0, job.SkippedResourceCounts["Patient"]);

            await blobClient.DeleteIfExistsAsync();
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("{entry: [{resource: {}}]}")]
        public async Task GivenInvalidSearchBundle_WhenExecuteTask_ExceptionShouldBeThrown(string invalidBundle)
        {
            var containerName = Guid.NewGuid().ToString("N");
            var taskExecutor = GetTaskExecutor(invalidBundle, containerName);

            var typeFilters = new List<TypeFilter> { new ("Patient", null) };
            var filterInfo = new FilterInfo(FilterScope.System, null, DateTimeOffset.MinValue, typeFilters, null);

            // Create an active job.
            var activeJob = Job.Create(
                containerName,
                JobStatus.Running,
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                filterInfo);
            var taskContext = TaskContext.CreateFromJob(activeJob, typeFilters);

            var jobUpdater = GetJobUpdater(activeJob);
            await Assert.ThrowsAsync<FhirDataParseExeption>(() => taskExecutor.ExecuteAsync(taskContext, jobUpdater));
        }

        private static IFhirDataClient GetMockFhirDataClient(string firstBundle)
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            // Get bundle from next link
            string nextBundle = TestDataProvider.GetBundleFromFile(TestDataConstants.PatientBundleFile2);
            dataClient.SearchAsync(default, default).ReturnsForAnyArgs(firstBundle, nextBundle);
            return dataClient;
        }

        private static IParquetSchemaProvider ParquetSchemaProviderDelegate(string name)
        {
            return new LocalDefaultSchemaProvider(NullLogger<LocalDefaultSchemaProvider>.Instance);
        }

        private static IFhirSchemaManager<FhirParquetSchemaNode> GetFhirSchemaManager()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = TestUtils.TestSchemaDirectoryPath,
            });

            return new FhirParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, NullLogger<FhirParquetSchemaManager>.Instance);
        }

        private static ParquetDataProcessor GetParquetDataProcessor()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = TestUtils.TestSchemaDirectoryPath,
            });

            var fhirSchemaManager = new FhirParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, NullLogger<FhirParquetSchemaManager>.Instance);
            var arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            return new ParquetDataProcessor(
            fhirSchemaManager,
            arrowConfigurationOptions,
            NullLogger<ParquetDataProcessor>.Instance);
        }

        private IFhirDataWriter GetDataWriter(string containerName)
        {
            var containerFactory = new AzureBlobContainerClientFactory(new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()) ,new NullLoggerFactory());
            var storageConfig = new DataLakeStoreConfiguration
            {
                StorageUrl = TestBlobEndpoint,
            };
            var jobConfig = new JobConfiguration
            {
                ContainerName = containerName,
            };

            var dataSink = new AzureBlobDataSink(Options.Create(storageConfig), Options.Create(jobConfig));
            return new AzureBlobDataWriter(containerFactory, dataSink, new NullLogger<AzureBlobDataWriter>());
        }

        private ITaskExecutor GetTaskExecutor(string bundleResult, string containerName)
        {
            return new TaskExecutor(GetMockFhirDataClient(bundleResult), GetDataWriter(containerName), GetParquetDataProcessor(), GetFhirSchemaManager(), new NullLogger<TaskExecutor>());
        }

        private JobProgressUpdater GetJobUpdater(Job job)
        {
            var containerFactory = new AzureBlobContainerClientFactory(new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()), new NullLoggerFactory());
            var storageConfig = new DataLakeStoreConfiguration
            {
                StorageUrl = TestBlobEndpoint,
            };
            var jobConfig = new JobConfiguration
            {
                ContainerName = job.ContainerName,
            };

            var jobStore = new AzureBlobJobStore(
                containerFactory,
                Options.Create(jobConfig),
                Options.Create(storageConfig),
                new NullLogger<AzureBlobJobStore>());

            return new JobProgressUpdater(jobStore, job, new NullLogger<JobProgressUpdater>());
        }
    }
}
