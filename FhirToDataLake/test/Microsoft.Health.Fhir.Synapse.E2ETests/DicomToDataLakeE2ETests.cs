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
using Azure;
using Azure.Data.Tables;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Queues;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.JobManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.Tool;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Microsoft.Health.Fhir.Synapse.E2ETests
{
    [Collection("E2E Tests")]
    public class DicomToDataLakeE2ETests
    {
        private readonly BlobServiceClient _blobServiceClient;
        private readonly ITestOutputHelper _testOutputHelper;

        private const string TestConfigurationPath = "appsettings.dicomtodatalake.test.json";

        private const string ExpectedDataFolder = "TestData/Expected";

        private const byte QueueTypeByte = (byte)QueueType.DicomToDataLake;

        private BlobContainerClient _blobContainerClient;
        private IMetadataStore _metadataStore;
        private AzureStorageJobQueueClient<DicomToDataLakeAzureStorageJobInfo> _queueClient;
        private AzureStorageClientFactory _queueClientFactory;

        /// <summary>
        /// Initializes a new instance of the <see cref="DicomToDataLakeE2ETests"/> class.
        /// </summary>
        /// <param name="testOutputHelper">output helper.</param>
        public DicomToDataLakeE2ETests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;

            string storageUri = Environment.GetEnvironmentVariable("dataLakeStore:storageUrl");
            if (!string.IsNullOrWhiteSpace(storageUri))
            {
                _testOutputHelper.WriteLine($"Using custom data lake storage uri {storageUri}");
                _blobServiceClient = new BlobServiceClient(new Uri(storageUri), new DefaultAzureCredential());
            }
        }

        [SkippableFact]
        public async Task GivenValidDicomServer_WhenRun_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            await InitializeUniqueStorage();

            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();

            try
            {
                // Run e2e
                using var tokenSource = new CancellationTokenSource();
                IHost host = CreateHostBuilder(configuration).Build();
                Task hostRunTask = host.RunAsync(tokenSource.Token);

                CurrentTriggerEntity triggerEntity;
                while (true)
                {
                    await Task.Delay(TimeSpan.FromSeconds(10), CancellationToken.None);
                    try
                    {
                        triggerEntity = await _metadataStore.GetCurrentTriggerEntityAsync(QueueTypeByte, CancellationToken.None);
                        if (triggerEntity.TriggerStatus == TriggerStatus.Completed)
                        {
                            break;
                        }
                    }
                    catch
                    {
                        // ignored
                    }
                }

                tokenSource.Cancel();
                await hostRunTask;

                // check trigger
                Assert.NotNull(triggerEntity);
                Assert.Equal(TriggerStatus.Completed, triggerEntity.TriggerStatus);
                Assert.Equal(0, triggerEntity.TriggerSequenceId);

                long orchestratorJobId = triggerEntity.OrchestratorJobId;

                // Check job status
                JobInfo jobInfo = await _queueClient.GetJobByIdAsync(QueueTypeByte, orchestratorJobId, true, CancellationToken.None);
                CheckJobStatus(jobInfo, "Dicom_All.json");

                // Check result files
                Assert.Equal(13, await GetResultFileCount(_blobContainerClient, "result/Dicom"));
            }
            finally
            {
                await CleanStorage();
            }
        }

        private async Task InitializeUniqueStorage()
        {
            string uniqueName = Guid.NewGuid().ToString("N");
            string jobInfoTableName = $"jobinfotable{uniqueName}";
            string jobInfoQueueName = $"jobinfoqueue{uniqueName}";
            string metadataTableName = $"metadatatable{uniqueName}";

            _blobContainerClient = _blobServiceClient.GetBlobContainerClient(uniqueName);
            IOptions<JobConfiguration> jobConfig = Options.Create(new JobConfiguration
            {
                JobInfoTableName = jobInfoTableName,
                MetadataTableName = metadataTableName,
                JobInfoQueueName = jobInfoQueueName,
            });

            // Make sure the container is deleted before running the tests
            Assert.False(await _blobContainerClient.ExistsAsync());
            var azureTableClientFactory = new AzureTableClientFactory(
                new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()));

            _metadataStore = new AzureTableMetadataStore(azureTableClientFactory, jobConfig, new NullLogger<AzureTableMetadataStore>());
            Assert.True(_metadataStore.IsInitialized());
            _queueClientFactory = new AzureStorageClientFactory(
                jobInfoTableName,
                jobInfoQueueName,
                new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()),
                new NullLogger<AzureStorageClientFactory>());

            _queueClient = new AzureStorageJobQueueClient<DicomToDataLakeAzureStorageJobInfo>(
                _queueClientFactory,
                new NullLogger<AzureStorageJobQueueClient<DicomToDataLakeAzureStorageJobInfo>>());

            // set configuration
            Environment.SetEnvironmentVariable("job:containerName", uniqueName);
            Environment.SetEnvironmentVariable("job:metadataTableName", metadataTableName);
            Environment.SetEnvironmentVariable("job:jobInfoTableName", jobInfoTableName);
            Environment.SetEnvironmentVariable("job:jobInfoQueueName", jobInfoQueueName);
        }

        private async Task CleanStorage()
        {
            await _blobContainerClient.DeleteIfExistsAsync();
            await _metadataStore.DeleteMetadataTableAsync();
            TableClient jobInfoTableClient = _queueClientFactory.CreateTableClient();
            QueueClient jobInfoQueueClient = _queueClientFactory.CreateQueueClient();
            await jobInfoQueueClient.DeleteIfExistsAsync();
            await jobInfoTableClient.DeleteAsync();
        }

        private static void CheckJobStatus(JobInfo jobInfo, string expectedResultFile)
        {
            Assert.NotNull(jobInfo);
            Assert.Equal(JobStatus.Completed, jobInfo.Status);
            Assert.False(jobInfo.CancelRequested);

            // check result
            string fileName = Path.Combine(ExpectedDataFolder, expectedResultFile);
            var expectedResult = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobResult>(File.ReadAllText(fileName));
            var result = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobResult>(jobInfo.Result);

            Assert.NotNull(expectedResult);
            Assert.NotNull(result);
            Assert.Equal(expectedResult.CreatedJobCount, result.CreatedJobCount);
            Assert.Equal(expectedResult.NextOffset, result.NextOffset);
            Assert.Empty(result.RunningJobIds);
            Assert.True(DictionaryEquals(expectedResult.TotalResourceCounts, result.TotalResourceCounts));
            Assert.True(DictionaryEquals(expectedResult.ProcessedResourceCounts, result.ProcessedResourceCounts));
            Assert.True(DictionaryEquals(expectedResult.SkippedResourceCounts, result.SkippedResourceCounts));
        }

        private async Task<int> GetResultFileCount(BlobContainerClient blobContainerClient, string filePrefix)
        {
            int resultFileCount = 0;
            await foreach (Page<BlobHierarchyItem> page in blobContainerClient.GetBlobsByHierarchyAsync(prefix: filePrefix).AsPages())
            {
                foreach (BlobHierarchyItem blobItem in page.Values)
                {
                    _testOutputHelper.WriteLine($"Getting result file {blobItem.Blob.Name}.");

                    if (blobItem.Blob.Name.EndsWith(".parquet"))
                    {
                        resultFileCount++;
                    }
                }
            }

            _testOutputHelper.WriteLine($"Getting result file count {resultFileCount}.");
            return resultFileCount;
        }

        private static bool DictionaryEquals(
            Dictionary<string, int> expectedDictionary,
            Dictionary<string, int> actualDictionary)
        {
            if (expectedDictionary.Count != actualDictionary.Count)
            {
                return false;
            }

            if (expectedDictionary.Keys.Except(actualDictionary.Keys).Any())
            {
                return false;
            }

            if (actualDictionary.Keys.Except(expectedDictionary.Keys).Any())
            {
                return false;
            }

            foreach (KeyValuePair<string, int> pair in actualDictionary)
            {
                if (expectedDictionary[pair.Key] != pair.Value)
                {
                    return false;
                }
            }

            return true;
        }

        private static IHostBuilder CreateHostBuilder(IConfiguration configuration) =>
            Host.CreateDefaultBuilder()
                .ConfigureServices((context, services) =>
                    services
                        .AddConfiguration(configuration)
                        .AddAzure()
                        .AddJobScheduler()
                        .AddJobManagement()
                        .AddDataSource()
                        .AddDataWriter()
                        .AddSchema()
                        .AddMetricsLogger()
                        .AddDiagnosticLogger()
                        .AddHostedService<SynapseLinkService>());
    }
}