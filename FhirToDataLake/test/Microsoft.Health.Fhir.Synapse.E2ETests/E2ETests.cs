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
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core;
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

        private const string _configurationPath = "appsettings.test.json";
        private const int TriggerIntervalInMinutes = 5;

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
            Environment.SetEnvironmentVariable("filter:requiredTypes", "Patient,Observation");

            var configuration = new ConfigurationBuilder()
                .AddJsonFile(_configurationPath)
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
                Assert.Equal(4, await GetResultFileCount(blobContainerClient, "result/Observation/2022/07/01"));
                Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/Patient/2022/07/01"));
            }
            finally
            {
                blobContainerClient.DeleteIfExists();
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

            var configuration = new ConfigurationBuilder()
                .AddJsonFile(_configurationPath)
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
                Assert.Equal(14, await GetResultFileCount(blobContainerClient, "result"));
            }
            finally
            {
                blobContainerClient.DeleteIfExists();
            }
        }

        // TODO: enable to test incremental search when script is ready
        /*
        [SkippableFact]
        public async Task GivenRecentDateRange_WhenProcess_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            BlobContainerClient blobContainerClient = _blobServiceClient.GetBlobContainerClient(uniqueContainerName);

            // Make sure the container is deleted before running the tests
            Assert.False(await blobContainerClient.ExistsAsync());

            // Load configuration and set endTime to yesterday
            Environment.SetEnvironmentVariable("job:containerName", uniqueContainerName);
            var configuration = new ConfigurationBuilder()
                .AddJsonFile(_configurationPath)
                .AddEnvironmentVariables()
                .Build();
            var now = DateTime.UtcNow;
            configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["startTime"] = now.AddMinutes(-1 * TriggerIntervalInMinutes).ToString("o");
            configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"] = now.ToString("o");
            configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["containerName"] = uniqueContainerName;

            try
            {
                // Run e2e
                var host = CreateHostBuilder(configuration).Build();
                await host.RunAsync();

                // Check job status
                var fileName = Path.Combine(_expectedDataFolder, "System_Patient_Observation.json");
                var expectedJob = JsonConvert.DeserializeObject<Job>(File.ReadAllText(fileName));

                await CheckJobStatus(blobContainerClient, expectedJob);

                // Check parquet files
                Assert.Equal(1, await GetResultFileCount(blobContainerClient, "result/Observation/2000/09/01"));
                Assert.Equal(0, await GetResultFileCount(blobContainerClient, "result/Patient/2000/09/01"));
            }
            finally
            {
                _testOutputHelper.WriteLine("Dispose.");
                blobContainerClient.DeleteIfExists();
            }
        }
        */
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
                    Assert.Equal(expectedJob.FilterContext.FilterScope, completedJob.FilterContext.FilterScope);
                    Assert.Equal(expectedJob.FilterContext.GroupId, completedJob.FilterContext.GroupId);
                    Assert.True(completedJob.FilterContext.ProcessedPatientIds.ToHashSet().SetEquals(expectedJob.FilterContext.ProcessedPatientIds.ToHashSet()));
                    Assert.Equal(expectedJob.FilterContext.TypeFilters.Count(), completedJob.FilterContext.TypeFilters.Count());
                    Assert.Equal(expectedJob.FilterContext.TypeFilters.Count(), completedJob.FilterContext.TypeFilters.Count());
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
