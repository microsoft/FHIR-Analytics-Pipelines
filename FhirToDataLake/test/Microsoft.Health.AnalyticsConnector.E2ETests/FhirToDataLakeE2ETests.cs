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
using Microsoft.Health.AnalyticsConnector.Common;
using Microsoft.Health.AnalyticsConnector.Common.Authentication;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;
using Microsoft.Health.AnalyticsConnector.Common.Extensions;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;
using Microsoft.Health.AnalyticsConnector.Common.Models.Jobs;
using Microsoft.Health.AnalyticsConnector.Core;
using Microsoft.Health.AnalyticsConnector.Core.Jobs;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataWriter;
using Microsoft.Health.AnalyticsConnector.JobManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;
using Microsoft.Health.AnalyticsConnector.Tool;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Microsoft.Health.AnalyticsConnector.E2ETests
{
    [Collection("E2E Tests")]
    public class FhirToDataLakeE2ETests
    {
        private readonly BlobServiceClient _blobServiceClient;
        private readonly ITestOutputHelper _testOutputHelper;

        private const string TestConfigurationPath = "appsettings.fhirtodatalake.test.json";

        private const string ExpectedDataFolder = "TestData/Expected";

        private const byte QueueTypeByte = (byte)QueueType.FhirToDataLake;

        private BlobContainerClient _blobContainerClient;
        private IMetadataStore _metadataStore;
        private AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo> _queueClient;
        private AzureStorageClientFactory _queueClientFactory;

        /// <summary>
        /// Initializes a new instance of the <see cref="FhirToDataLakeE2ETests"/> class.
        /// </summary>
        /// <param name="testOutputHelper">output helper.</param>
        public FhirToDataLakeE2ETests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;

            string storageUri = Environment.GetEnvironmentVariable("dataLakeStore:storageUrl");
            if (!string.IsNullOrWhiteSpace(storageUri))
            {
                _testOutputHelper.WriteLine($"Using custom data lake storage uri {storageUri}");
                _blobServiceClient = new BlobServiceClient(new Uri(storageUri), new DefaultAzureCredential());
            }
        }

        [Fact]
        public void GivenInvalidFilterScope_WhenBuildHost_ExceptionShouldBeThrown()
        {
            Environment.SetEnvironmentVariable("filter:filterScope", "Unsupported");
            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();
            Assert.Throws<ConfigurationErrorException>(() => CreateHostBuilder(configuration).Build());

            // set the filter scope to default value
            Environment.SetEnvironmentVariable("filter:filterScope", "System");
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void GivenEmptyGroupId_WhenBuildHostForGroupFilterScope_ExceptionShouldBeThrown(string groupId)
        {
            Environment.SetEnvironmentVariable("filter:filterScope", "Group");
            Environment.SetEnvironmentVariable("filter:groupId", groupId);

            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();
            Assert.Throws<ConfigurationErrorException>(() => CreateHostBuilder(configuration).Build());
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("invalidGroupId")]
        [InlineData("d73baac10af90c4b931357b2e8ef9d8b")]
        public void GivenAnyGroupId_WhenBuildHostForSystemFilterScope_NoExceptionShouldBeThrown(string groupId)
        {
            Environment.SetEnvironmentVariable("filter:filterScope", "System");
            Environment.SetEnvironmentVariable("filter:groupId", groupId);

            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();
            Exception exception = Record.Exception(() => CreateHostBuilder(configuration).Build());
            Assert.Null(exception);
        }

        [SkippableFact]
        public async Task GivenRequiredTypes_WhenProcessSystemScope_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            await InitializeUniqueStorage();

            // specified configuration
            Environment.SetEnvironmentVariable("filter:filterScope", "System");
            Environment.SetEnvironmentVariable("filter:requiredTypes", "Patient,Observation");
            Environment.SetEnvironmentVariable("filter:typeFilters", string.Empty);

            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();

            DateTimeOffset endTime = DateTimeOffset.Parse(configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"]);

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
                        if (triggerEntity.TriggerStatus == TriggerStatus.Completed &&
                            triggerEntity.TriggerEndTime >= endTime)
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
                CheckJobStatus(jobInfo, "SystemScope_Patient_Observation.json");

                // Check result files
                Assert.Equal(13, await GetResultFileCount(_blobContainerClient, "result/Observation"));
                Assert.Equal(1, await GetResultFileCount(_blobContainerClient, "result/Patient"));
            }
            finally
            {
                await CleanStorage();
            }
        }

        [SkippableFact]
        public async Task GivenOnePatientGroup_WhenProcessGroupScope_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            await InitializeUniqueStorage();
            Environment.SetEnvironmentVariable("filter:filterScope", "Group");

            // only patient cbe1a164-c5c8-65b4-747a-829a6bd4e85f is included in this group
            Environment.SetEnvironmentVariable("filter:groupId", "af3aba4f-7bb6-41e3-85e4-d931d7653ede");
            Environment.SetEnvironmentVariable("filter:requiredTypes", string.Empty);
            Environment.SetEnvironmentVariable("filter:typeFilters", string.Empty);

            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();

            DateTimeOffset endTime = DateTimeOffset.Parse(configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"]);

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
                        triggerEntity =
                            await _metadataStore.GetCurrentTriggerEntityAsync(QueueTypeByte, CancellationToken.None);
                        if (triggerEntity == null)
                        {
                            continue;
                        }

                        if (triggerEntity.TriggerStatus == TriggerStatus.Completed &&
                            triggerEntity.TriggerEndTime >= endTime)
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
                JobInfo jobInfo =
                    await _queueClient.GetJobByIdAsync(QueueTypeByte, orchestratorJobId, true, CancellationToken.None);
                CheckJobStatus(jobInfo, "GroupScope_OnePatient_All.json");

                // Check result files
                Assert.Equal(16, await GetResultFileCount(_blobContainerClient, "result"));

                Dictionary<string, long> patientVersions =
                    await _metadataStore.GetPatientVersionsAsync(QueueTypeByte, CancellationToken.None);

                Assert.Single(patientVersions);
            }
            finally
            {
                await CleanStorage();
            }
        }

        [SkippableFact]
        public async Task GivenAllPatientGroupWithFilters_WhenProcessGroupScope_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            await InitializeUniqueStorage();

            // set configuration
            Environment.SetEnvironmentVariable("filter:filterScope", "Group");
            Environment.SetEnvironmentVariable("filter:requiredTypes", "Condition,MedicationRequest,Patient");
            Environment.SetEnvironmentVariable("filter:typeFilters", "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z");

            // this group includes all the 80 patients
            Environment.SetEnvironmentVariable("filter:groupId", "72d653ce-2dbb-4432-bfa0-9ac47d0e0a2c");

            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();

            DateTimeOffset endTime = DateTimeOffset.Parse(configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"]);

            try
            {
                // Run e2e
                using var tokenSource = new CancellationTokenSource();
                IHost host = CreateHostBuilder(configuration).Build();
                Task hostRunTask = host.RunAsync(tokenSource.Token);

                CurrentTriggerEntity triggerEntity = await WaitJobCompleted(endTime);

                tokenSource.Cancel();
                await hostRunTask;

                // check trigger
                Assert.NotNull(triggerEntity);
                Assert.Equal(TriggerStatus.Completed, triggerEntity.TriggerStatus);
                Assert.Equal(0, triggerEntity.TriggerSequenceId);

                long orchestratorJobId = triggerEntity.OrchestratorJobId;

                // Check job status
                JobInfo jobInfo = await _queueClient.GetJobByIdAsync(QueueTypeByte, orchestratorJobId, true, CancellationToken.None);
                CheckJobStatus(jobInfo, "GroupScope_AllPatient_Filters.json");

                // Check result files
                Assert.Equal(2, await GetResultFileCount(_blobContainerClient, "result/Patient/2022/07/01"));
                Assert.Equal(2, await GetResultFileCount(_blobContainerClient, "result/Condition/2022/07/01"));
                Assert.Equal(2, await GetResultFileCount(_blobContainerClient, "result/MedicationRequest/2022/07/01"));

                // Check patient version
                Dictionary<string, long> patientVersions =
                    await _metadataStore.GetPatientVersionsAsync(QueueTypeByte, CancellationToken.None);

                Assert.Equal(80, patientVersions.Count);
                foreach (KeyValuePair<string, long> kv in patientVersions)
                {
                    Assert.Equal(1, kv.Value);
                }
            }
            finally
            {
                await CleanStorage();
            }
        }

        [SkippableFact]
        public async Task GivenTwoEndTimes_WhenProcessIncrementalData_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            await InitializeUniqueStorage();

            // Set configuration
            Environment.SetEnvironmentVariable("filter:filterScope", "Group");
            Environment.SetEnvironmentVariable("filter:requiredTypes", "Condition,MedicationRequest,Patient");
            Environment.SetEnvironmentVariable("filter:typeFilters", "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z");

            // this group includes all the 80 patients
            Environment.SetEnvironmentVariable("filter:groupId", "72d653ce-2dbb-4432-bfa0-9ac47d0e0a2c");

            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();

            // set end time to the time that not all the resources are imported.
            configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"] = "2022-06-29T16:00:00.000Z";

            // set schedulerCronExpression, so there are three triggers
            configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["schedulerCronExpression"] = "0 0 0 * * *";

            DateTimeOffset endTime = DateTimeOffset.Parse(configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"]);

            try
            {
                // trigger first time, only the resources imported before end time are synced.
                using var tokenSource = new CancellationTokenSource();
                IHost host = CreateHostBuilder(configuration).Build();
                Task hostRunTask = host.RunAsync(tokenSource.Token);

                CurrentTriggerEntity triggerEntity = await WaitJobCompleted(endTime);

                tokenSource.Cancel();
                await hostRunTask;

                // check trigger
                Assert.NotNull(triggerEntity);
                Assert.Equal(TriggerStatus.Completed, triggerEntity.TriggerStatus);
                Assert.Equal(0, triggerEntity.TriggerSequenceId);

                long orchestratorJobId = triggerEntity.OrchestratorJobId;

                // Check job status
                JobInfo jobInfo = await _queueClient.GetJobByIdAsync(QueueTypeByte, orchestratorJobId, true, CancellationToken.None);
                CheckJobStatus(jobInfo, "GroupScope_AllPatient_Filters_part1.json");

                // modify the job end time to fake incremental sync.
                // the second triggered job should sync the other resources
                configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"] =
                    "2022-06-30T00:00:00.000Z";

                endTime = DateTimeOffset.Parse(
                    configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"]);

                using var tokenSource2 = new CancellationTokenSource();
                IHost host2 = CreateHostBuilder(configuration).Build();
                Task hostRunTask2 = host2.RunAsync(tokenSource2.Token);

                triggerEntity = await WaitJobCompleted(endTime);

                tokenSource2.Cancel();
                await hostRunTask2;

                // check trigger
                Assert.NotNull(triggerEntity);
                Assert.Equal(TriggerStatus.Completed, triggerEntity.TriggerStatus);
                Assert.Equal(1, triggerEntity.TriggerSequenceId);

                orchestratorJobId = triggerEntity.OrchestratorJobId;

                // Check job status
                jobInfo = await _queueClient.GetJobByIdAsync(QueueTypeByte, orchestratorJobId, true, CancellationToken.None);
                CheckJobStatus(jobInfo, "GroupScope_AllPatient_Filters_part2.json");

                // Check files
                Assert.Equal(2, await GetResultFileCount(_blobContainerClient, "result/Patient"));
                Assert.Equal(3, await GetResultFileCount(_blobContainerClient, "result/Condition"));
                Assert.Equal(3, await GetResultFileCount(_blobContainerClient, "result/MedicationRequest"));

                // check patient version
                Dictionary<string, long> patientVersions =
                    await _metadataStore.GetPatientVersionsAsync(QueueTypeByte, CancellationToken.None);

                Assert.Equal(80, patientVersions.Count);
                foreach (KeyValuePair<string, long> kv in patientVersions)
                {
                    Assert.Equal(1, kv.Value);
                }
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

            _queueClient = new AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo>(
                _queueClientFactory,
                new NullLogger<AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo>>());

            // set configuration
            Environment.SetEnvironmentVariable("job:containerName", uniqueName);
            Environment.SetEnvironmentVariable("job:metadataTableName", metadataTableName);
            Environment.SetEnvironmentVariable("job:jobInfoTableName", jobInfoTableName);
            Environment.SetEnvironmentVariable("job:jobInfoQueueName", jobInfoQueueName);
        }

        private async Task<CurrentTriggerEntity> WaitJobCompleted(DateTimeOffset configurationEndTime)
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(10), CancellationToken.None);
                try
                {
                    CurrentTriggerEntity triggerEntity = await _metadataStore.GetCurrentTriggerEntityAsync(QueueTypeByte, CancellationToken.None);
                    if (triggerEntity.TriggerStatus == TriggerStatus.Completed &&
                        triggerEntity.TriggerEndTime >= configurationEndTime)
                    {
                        return triggerEntity;
                    }
                }
                catch
                {
                    // ignored
                }
            }
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
            var expectedResult = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobResult>(File.ReadAllText(fileName));
            var result = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobResult>(jobInfo.Result);

            Assert.NotNull(expectedResult);
            Assert.NotNull(result);
            Assert.Equal(expectedResult.CreatedJobCount, result.CreatedJobCount);
            Assert.Equal(expectedResult.NextPatientIndex, result.NextPatientIndex);
            Assert.Equal(expectedResult.NextJobTimestamp, result.NextJobTimestamp);
            Assert.Equal(expectedResult.SubmittedResourceTimestamps, result.SubmittedResourceTimestamps);
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