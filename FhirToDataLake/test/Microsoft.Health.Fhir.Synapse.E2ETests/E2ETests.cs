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
using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.Tool;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace Microsoft.Health.Fhir.Synapse.E2ETests
{
    public class E2ETests
    {
        private readonly BlobServiceClient _blobServiceClient;
        private readonly ITestOutputHelper _testOutputHelper;

        private const string TestConfigurationPath = "appsettings.test.json";

        private const string _expectedDataFolder = "TestData/Expected";

        /// <summary>
        /// Initializes a new instance of the <see cref="E2ETests"/> class.
        /// To run the tests locally, pull healthplatformregistry.azurecr.io/fhir-analytics-data-source:v0.0.1 and run it in port 5000.
        /// </summary>
        /// <param name="testOutputHelper">output helper.</param>
        public E2ETests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            var storageUri = Environment.GetEnvironmentVariable("dataLakeStore:storageUrl");
            if (!string.IsNullOrEmpty(storageUri))
            {
                _testOutputHelper.WriteLine($"Using custom data lake storage uri {storageUri}");
                _blobServiceClient = new BlobServiceClient(new Uri(storageUri), new DefaultAzureCredential());
            }
        }

        [Fact]
        public void GivenInvalidFilterScope_WhenBuildHost_ExceptionShouldBeThrown()
        {
            Environment.SetEnvironmentVariable("filter:filterScope", "Unsupported");
            var configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();
            Assert.Throws<ConfigurationErrorException>(() => CreateHostBuilder(configuration).Build());
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void GivenEmptyGroupId_WhenBuildHostForGroupFilterScope_ExceptionShouldBeThrown(string groupId)
        {
            Environment.SetEnvironmentVariable("filter:filterScope", "Group");
            Environment.SetEnvironmentVariable("filter:groupId", groupId);

            var configuration = new ConfigurationBuilder()
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

            var configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();
            var exception = Record.Exception(() => CreateHostBuilder(configuration).Build());
            Assert.Null(exception);
        }

        [SkippableFact]
        public async Task GivenRequiredTypes_WhenProcessSystemScope_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            BlobContainerClient blobContainerClient = _blobServiceClient.GetBlobContainerClient(uniqueContainerName);

            // Make sure the container is deleted before running the tests
            Assert.False(await blobContainerClient.ExistsAsync());

            // Load configuration
            Environment.SetEnvironmentVariable("job:containerName", uniqueContainerName);

            Environment.SetEnvironmentVariable("filter:filterScope", "System");
            Environment.SetEnvironmentVariable("filter:requiredTypes", "Patient,Observation");
            Environment.SetEnvironmentVariable("filter:typeFilters", string.Empty);

            var configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();

            try
            {
                // Run e2e
                var host = CreateHostBuilder(configuration).Build();
                await host.RunAsync();

                // Check job status
                var fileName = Path.Combine(_expectedDataFolder, "SystemScope_Patient_Observation.json");
                var expectedJob = JsonConvert.DeserializeObject<Job>(File.ReadAllText(fileName));

                await CheckJobStatus(blobContainerClient, expectedJob);

                // Check result files
                Assert.Equal(8, await GetResultFileCount(blobContainerClient, "result/Observation/2022/07/01"));
                Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/Patient/2022/07/01"));
            }
            finally
            {
                await blobContainerClient.DeleteIfExistsAsync();
            }
        }

        [SkippableFact]
        public async Task GivenOnePatientGroup_WhenProcessGroupScope_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            BlobContainerClient blobContainerClient = _blobServiceClient.GetBlobContainerClient(uniqueContainerName);

            // Make sure the container is deleted before running the tests
            Assert.False(await blobContainerClient.ExistsAsync());

            // Load configuration
            Environment.SetEnvironmentVariable("job:containerName", uniqueContainerName);

            Environment.SetEnvironmentVariable("filter:filterScope", "Group");

            // only patient cbe1a164-c5c8-65b4-747a-829a6bd4e85f is included in this group
            Environment.SetEnvironmentVariable("filter:groupId", "af3aba4f-7bb6-41e3-85e4-d931d7653ede");
            Environment.SetEnvironmentVariable("filter:requiredTypes", string.Empty);
            Environment.SetEnvironmentVariable("filter:typeFilters", string.Empty);

            var configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();

            try
            {
                // Run e2e
                var host = CreateHostBuilder(configuration).Build();
                await host.RunAsync();

                // Check job status
                var fileName = Path.Combine(_expectedDataFolder, "GroupScope_OnePatient_All.json");
                var expectedJob = JsonConvert.DeserializeObject<Job>(File.ReadAllText(fileName));

                await CheckJobStatus(blobContainerClient, expectedJob);

                // Check result files
                Assert.Equal(20, await GetResultFileCount(blobContainerClient, "result"));

                var schedulerMetadata = await GetSchedulerMetadata(blobContainerClient);

                Assert.Empty(schedulerMetadata.FailedJobs);
                Assert.Single(schedulerMetadata.ProcessedPatients);
            }
            finally
            {
                await blobContainerClient.DeleteIfExistsAsync();
            }
        }

        [SkippableFact]
        public async Task GivenAllPatientGroupWithFilters_WhenProcessGroupScope_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            BlobContainerClient blobContainerClient = _blobServiceClient.GetBlobContainerClient(uniqueContainerName);

            // Make sure the container is deleted before running the tests
            Assert.False(await blobContainerClient.ExistsAsync());

            // Load configuration
            Environment.SetEnvironmentVariable("job:containerName", uniqueContainerName);
            Environment.SetEnvironmentVariable("filter:filterScope", "Group");
            Environment.SetEnvironmentVariable("filter:requiredTypes", "Condition,MedicationRequest,Patient");
            Environment.SetEnvironmentVariable("filter:typeFilters", "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z");

            // this group includes all the 80 patients
            Environment.SetEnvironmentVariable("filter:groupId", "72d653ce-2dbb-4432-bfa0-9ac47d0e0a2c");

            var configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();

            try
            {
                // Run e2e
                var host = CreateHostBuilder(configuration).Build();
                await host.RunAsync();

                // Check job status
                var fileName = Path.Combine(_expectedDataFolder, "GroupScope_AllPatient_Filters.json");
                var expectedJob = JsonConvert.DeserializeObject<Job>(File.ReadAllText(fileName));

                await CheckJobStatus(blobContainerClient, expectedJob);

                // Check result files
                Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/Patient/2022/07/01"));
                Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/Condition/2022/07/01"));
                Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/MedicationRequest/2022/07/01"));

                var schedulerMetadata = await GetSchedulerMetadata(blobContainerClient);

                Assert.Empty(schedulerMetadata.FailedJobs);
                Assert.Equal(80, schedulerMetadata.ProcessedPatients.Count());
            }
            finally
            {
                await blobContainerClient.DeleteIfExistsAsync();
            }
        }

        [SkippableFact]
        public async Task GivenTwoEndTimes_WhenProcessIncrementalData_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            BlobContainerClient blobContainerClient = _blobServiceClient.GetBlobContainerClient(uniqueContainerName);

            // Make sure the container is deleted before running the tests
            Assert.False(await blobContainerClient.ExistsAsync());

            // Load configuration
            Environment.SetEnvironmentVariable("job:containerName", uniqueContainerName);
            Environment.SetEnvironmentVariable("filter:filterScope", "Group");
            Environment.SetEnvironmentVariable("filter:requiredTypes", "Condition,MedicationRequest,Patient");
            Environment.SetEnvironmentVariable("filter:typeFilters", "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z");

            // this group includes all the 80 patients
            Environment.SetEnvironmentVariable("filter:groupId", "72d653ce-2dbb-4432-bfa0-9ac47d0e0a2c");

            var configuration = new ConfigurationBuilder()
                .AddJsonFile(TestConfigurationPath)
                .AddEnvironmentVariables()
                .Build();

            // set end time to the time that not all the resources are imported.
            configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"] = "2022-06-29T16:00:00.000Z";

            try
            {
                // trigger first time, only the resources imported before end time are synced.
                var host_1 = CreateHostBuilder(configuration).Build();
                await host_1.RunAsync();

                // Check job status
                var fileName = Path.Combine(_expectedDataFolder, "GroupScope_AllPatient_Filters_part1.json");
                var expectedJob = JsonConvert.DeserializeObject<Job>(File.ReadAllText(fileName));

                await CheckJobStatus(blobContainerClient, expectedJob);

                // modify the job end time to fake incremental sync.
                // the second triggered job should sync the other resources
                configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"] =
                    "2022-07-01T00:00:00.000Z";

                var host_2 = CreateHostBuilder(configuration).Build();
                await host_2.RunAsync();

                var completedJobCount = 0;
                Dictionary<string, int> totalResourceCount = new Dictionary<string, int>();
                await foreach (var blobItem in blobContainerClient.GetBlobsAsync(prefix: "jobs/completedJobs"))
                {
                    _testOutputHelper.WriteLine($"Queried blob {blobItem.Name}.");

                    if (blobItem.Name.EndsWith(".json"))
                    {
                        completedJobCount++;
                        var blobClient = blobContainerClient.GetBlobClient(blobItem.Name);
                        var blobDownloadInfo = await blobClient.DownloadAsync();
                        using var reader = new StreamReader(blobDownloadInfo.Value.Content, Encoding.UTF8);
                        var completedJob = JsonConvert.DeserializeObject<Job>(await reader.ReadToEndAsync());

                        Assert.Equal(JobStatus.Succeeded, completedJob.Status);

                        totalResourceCount =
                            totalResourceCount.ConcatDictionaryCount(completedJob.TotalResourceCounts);

                        // Check parquet files
                        Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/Patient/2022/06/29"));
                        Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/Condition/2022/06/29"));
                        Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/Condition/2022/07/01"));
                        Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/MedicationRequest/2022/06/29"));
                        Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/MedicationRequest/2022/07/01"));

                        var schedulerMetadata = await GetSchedulerMetadata(blobContainerClient);

                        Assert.Empty(schedulerMetadata.FailedJobs);
                        Assert.Equal(80, schedulerMetadata.ProcessedPatients.Count());
                    }
                }

                // there should be two completed jobs
                Assert.Equal(2, completedJobCount);

                var allResourceFileName = Path.Combine(_expectedDataFolder, "GroupScope_AllPatient_Filters.json");
                var allResourceJob = JsonConvert.DeserializeObject<Job>(File.ReadAllText(allResourceFileName));

                // the total resource count of these two job should equal to all the resources count
                Assert.True(DictionaryEquals(allResourceJob.TotalResourceCounts, totalResourceCount));
            }
            finally
            {
                _testOutputHelper.WriteLine("Dispose.");
                blobContainerClient.DeleteIfExists();
            }
        }

        private async Task<SchedulerMetadata> GetSchedulerMetadata(BlobContainerClient blobContainerClient)
        {
            var blobClient = blobContainerClient.GetBlobClient("jobs/scheduler.metadata");
            var blobDownloadInfo = await blobClient.DownloadAsync();
            using var reader = new StreamReader(blobDownloadInfo.Value.Content, Encoding.UTF8);
            return JsonConvert.DeserializeObject<SchedulerMetadata>(await reader.ReadToEndAsync());
        }

        private async Task<int> GetResultFileCount(BlobContainerClient blobContainerClient, string filePrefix)
        {
            var resultFileCount = 0;
            await foreach (var page in blobContainerClient.GetBlobsByHierarchyAsync(prefix: filePrefix).AsPages())
            {
                foreach (var blobItem in page.Values)
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

        private async Task CheckJobStatus(BlobContainerClient blobContainerClient, Job expectedJob)
        {
            var hasCompletedJobs = false;
            await foreach (var blobItem in blobContainerClient.GetBlobsAsync(prefix: "jobs/completedJobs"))
            {
                _testOutputHelper.WriteLine($"Queried blob {blobItem.Name}.");

                if (blobItem.Name.EndsWith(".json"))
                {
                    hasCompletedJobs = true;
                    var blobClient = blobContainerClient.GetBlobClient(blobItem.Name);
                    var blobDownloadInfo = await blobClient.DownloadAsync();
                    using var reader = new StreamReader(blobDownloadInfo.Value.Content, Encoding.UTF8);
                    var completedJob = JsonConvert.DeserializeObject<Job>(await reader.ReadToEndAsync());

                    // The status should be succeeded, which means succeeded
                    Assert.Equal(JobStatus.Succeeded, completedJob.Status);
                    Assert.Equal(expectedJob.FilterInfo.FilterScope, completedJob.FilterInfo.FilterScope);
                    if (completedJob.FilterInfo.FilterScope == FilterScope.Group)
                    {
                        Assert.Equal(expectedJob.FilterInfo.GroupId, completedJob.FilterInfo.GroupId);
                    }

                    Assert.True(completedJob.FilterInfo.ProcessedPatients.ToHashSet().SetEquals(expectedJob.FilterInfo.ProcessedPatients.ToHashSet()));
                    Assert.Equal(expectedJob.FilterInfo.TypeFilters.Count(), completedJob.FilterInfo.TypeFilters.Count());
                    Assert.Equal(expectedJob.FilterInfo.TypeFilters.Count(), completedJob.FilterInfo.TypeFilters.Count());
                    Assert.Empty(expectedJob.RunningTasks);

                    Assert.True(DictionaryEquals(expectedJob.TotalResourceCounts, completedJob.TotalResourceCounts));
                    Assert.True(DictionaryEquals(expectedJob.ProcessedResourceCounts, completedJob.ProcessedResourceCounts));
                    Assert.True(DictionaryEquals(expectedJob.SkippedResourceCounts, completedJob.SkippedResourceCounts));

                    break;
                }
            }

            _testOutputHelper.WriteLine($"Checked job status {hasCompletedJobs}.");

            Assert.True(hasCompletedJobs);
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

            foreach (var pair in actualDictionary)
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
                        .AddDataSource()
                        .AddSchema()
                        .AddHostedService<SynapseLinkService>());
    }
}
