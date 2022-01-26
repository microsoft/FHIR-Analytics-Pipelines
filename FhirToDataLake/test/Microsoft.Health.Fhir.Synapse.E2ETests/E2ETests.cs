// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Microsoft.Health.Fhir.Synapse.Core;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.Tool;
using Newtonsoft.Json.Linq;
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
        public async Task GivenPreviousDateRange_WhenProcess_CorrectResultShouldBeReturnedAsync()
        {
            Skip.If(_blobServiceClient == null);
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            BlobContainerClient blobContainerClient = _blobServiceClient.GetBlobContainerClient(uniqueContainerName);

            // Make sure the container is deleted before running the tests
            Assert.False(await blobContainerClient.ExistsAsync());

            // Load configuration
            Environment.SetEnvironmentVariable("job:containerName", uniqueContainerName);
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
                await CheckJobStatus(blobContainerClient);

                // Check result files
                Assert.Equal(2, await GetResultFileCount(blobContainerClient, "result/Observation/2000/09/01"));
                Assert.Equal(2, await GetResultFileCount(blobContainerClient, "result/Patient/2000/09/01"));
            }
            finally
            {
                blobContainerClient.DeleteIfExists();
            }
        }

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
                await CheckJobStatus(blobContainerClient);

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

        private async Task CheckJobStatus(BlobContainerClient blobContainerClient)
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
                    var content = await reader.ReadToEndAsync();

                    // The status should be 2, which means succeeded
                    Assert.Equal(2, JObject.Parse(content)["status"]?.Value<int>());

                    break;
                }
            }

            _testOutputHelper.WriteLine($"Checked job status {hasCompletedJobs}.");

            Assert.True(hasCompletedJobs);
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
