// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using EnsureThat.Enforcers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
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
            var taskExecutor = GetTaskExecutor(TestDataProvider.GetBundleFromFile("TestData/bundle1.json"), containerName);

            // Create an active job.
            var activeJob = new Job(
                containerName,
                JobStatus.Running,
                new List<string> { "Patient" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));
            var taskContext = TaskContext.Create("Patient", activeJob);

            var jobUpdater = GetJobUpdater(activeJob);
            var taskResult = await taskExecutor.ExecuteAsync(taskContext, jobUpdater);

            // verify task result;
            Assert.Null(taskResult.ContinuationToken);
            Assert.Equal(2, taskResult.PartId);
            Assert.Equal(3, taskResult.SearchCount);
            Assert.Equal(0, taskResult.SkippedCount);

            jobUpdater.Complete();
            await jobUpdater.Consume();

            // verify blob data;
            var blobClient = new BlobContainerClient(TestBlobEndpoint, containerName);
            var blobPages = blobClient.GetBlobs(prefix: "staging").AsPages();
            Assert.Equal(2, blobPages.First().Values.Count());

            // verify job data
            var jobBlob = blobClient.GetBlobClient($"{AzureBlobJobConstants.ActiveJobFolder}/{activeJob.Id}.json");
            using var stream = new MemoryStream();
            jobBlob.DownloadTo(stream);
            stream.Position = 0;
            using var streamReader = new StreamReader(stream);
            var jobContent = streamReader.ReadToEnd();
            var job = JsonConvert.DeserializeObject<Job>(jobContent);
            Assert.Equal(activeJob.Id, job.Id);
            Assert.Contains("Patient", job.CompletedResources);
            Assert.Equal(2, job.PartIds["Patient"]);
            Assert.Equal(3, job.ProcessedResourceCounts["Patient"]);
            Assert.Null(job.ResourceProgresses["Patient"]);

            blobClient.DeleteIfExists();
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("{entry: [{resource: {}}]}")]
        public async Task GivenInvalidSearchBundle_WhenExecuteTask_ExceptionShouldBeThrown(string invalidBundle)
        {
            var containerName = Guid.NewGuid().ToString("N");
            var taskExecutor = GetTaskExecutor(invalidBundle, containerName);

            // Create an active job.
            var activeJob = new Job(
                containerName,
                JobStatus.Running,
                new List<string> { "Patient" },
                new DataPeriod(DateTimeOffset.MinValue, DateTimeOffset.MaxValue),
                DateTimeOffset.Now.AddMinutes(-11));
            var taskContext = TaskContext.Create("Patient", activeJob);

            var jobUpdater = GetJobUpdater(activeJob);
            await Assert.ThrowsAsync<FhirDataParseExeption>(() => taskExecutor.ExecuteAsync(taskContext, jobUpdater));
        }

        private static IFhirDataClient GetMockFhirDataClient(string firstBundle)
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            // Get bundle from next link
            string nextBundle = TestDataProvider.GetBundleFromFile("TestData/bundle2.json");
            dataClient.SearchAsync(default, default).ReturnsForAnyArgs(firstBundle, nextBundle);
            return dataClient;
        }

        private static ParquetDataProcessor GetParquetDataProcessor()
        {
            var schemaConfigurationOption = Microsoft.Extensions.Options.Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = @"..\..\..\..\..\data\schemas",
            });

            var fhirSchemaManager = new FhirParquetSchemaManager(schemaConfigurationOption, NullLogger<FhirParquetSchemaManager>.Instance);
            var arrowConfigurationOptions = Microsoft.Extensions.Options.Options.Create(new ArrowConfiguration());

            return new ParquetDataProcessor(
            fhirSchemaManager,
            arrowConfigurationOptions,
            NullLogger<ParquetDataProcessor>.Instance);
        }

        private IFhirDataWriter GetDataWriter(string containerName)
        {
            var containerFactory = new AzureBlobContainerClientFactory(new NullLoggerFactory());
            var storageConfig = new DataLakeStoreConfiguration
            {
                StorageUrl = TestBlobEndpoint,
            };
            var jobConfig = new JobConfiguration
            {
                ContainerName = containerName,
            };

            var dataSink = new AzureBlobDataSink(
                Microsoft.Extensions.Options.Options.Create(storageConfig),
                Microsoft.Extensions.Options.Options.Create(jobConfig));
            return new AzureBlobDataWriter(containerFactory, dataSink, new NullLogger<AzureBlobDataWriter>());
        }

        private ITaskExecutor GetTaskExecutor(string bundleResult, string containerName)
        {
            return new TaskExecutor(GetMockFhirDataClient(bundleResult), GetDataWriter(containerName), GetParquetDataProcessor(), new NullLogger<TaskExecutor>());
        }

        private JobProgressUpdater GetJobUpdater(Job job)
        {
            var containerFactory = new AzureBlobContainerClientFactory(new NullLoggerFactory());
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
