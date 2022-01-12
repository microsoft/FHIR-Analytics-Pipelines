// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
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

namespace Microsoft.Health.Fhir.Synapse.E2ETests
{
    public class E2ETests
    {
        private readonly BlobServiceClient _blobServiceClient;
        private readonly BlobContainerClient _blobContainerClient;
        private const string _configurationPath = "appsettings.test.json";
        private int _triggerIntervalInMinutes = 5;

        /// <summary>
        /// Initializes a new instance of the <see cref="E2ETests"/> class.
        /// To run the tests locally, pull healthplatformregistry.azurecr.io/fhir-analytics-data-source:v0.0.1 and run it in port 5000.
        /// </summary>
        public E2ETests()
        {
            _blobServiceClient = new BlobServiceClient(TestConstants.AzureStorageEmulatorConnectionString);
            _blobContainerClient = _blobServiceClient.GetBlobContainerClient(TestConstants.TestContainerName);
        }

        [Fact]
        public async Task GivenPreviousDateRange_WhenProcess_CorrectResultShouldBeReturnedAsync()
        {
            // Reset test container
            await ResetTestContainerAsync();

            // Load configuration
            var configuration = new ConfigurationBuilder().AddJsonFile(_configurationPath).Build();

            // Run e2e
            var host = CreateHostBuilder(configuration).Build();
            await host.RunAsync();

            // Check job status
            CheckJobStatus();

            // Check result files
            Assert.Equal(2, await GetResultFileCount("result/Observation/2000/09/01/Observation"));
            Assert.Equal(2, await GetResultFileCount("result/Patient/2000/09/01/Patient"));
        }

        [Fact]
        public async Task GivenRecentDateRange_WhenProcess_CorrectResultShouldBeReturnedAsync()
        {
            // Reset test container
            await ResetTestContainerAsync();

            // Load configuration and set endTime to yesterday
            var configuration = new ConfigurationBuilder().AddJsonFile(_configurationPath).Build();
            var now = DateTime.UtcNow;
            configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["startTime"] = now.AddMinutes(-1 * _triggerIntervalInMinutes).ToString("o");
            configuration.GetSection(ConfigurationConstants.JobConfigurationKey)["endTime"] = now.ToString("o");

            // Run e2e
            var host = CreateHostBuilder(configuration).Build();
            await host.RunAsync();

            // Check job status
            CheckJobStatus();

            // Check parquet files
            Assert.Equal(1, await GetResultFileCount("result/Observation/2000/09/01/Observation"));
            Assert.Equal(0, await GetResultFileCount("result/Patient/2000/09/01/Patient"));
        }

        private async Task<int> GetResultFileCount(string filePrefix)
        {
            var resultFileCount = 0;
            await foreach (var blobItem in _blobContainerClient.GetBlobsAsync())
            {
                var blobName = blobItem.Name;
                if (blobName.StartsWith(filePrefix))
                {
                    resultFileCount++;
                }
            }

            return resultFileCount;
        }

        private async void CheckJobStatus()
        {
            var hasCompletedJobs = false;
            await foreach (var blobName in _blobContainerClient.GetBlobsAsync())
            {
                if (blobName.Name.StartsWith("jobs/completedJobs"))
                {
                    hasCompletedJobs = true;
                    var blobClient = _blobContainerClient.GetBlobClient(blobName.Name);
                    var blobDownloadInfo = await blobClient.DownloadAsync();
                    using var reader = new StreamReader(blobDownloadInfo.Value.Content, Encoding.UTF8);
                    var content = await reader.ReadToEndAsync();

                    // The status should be 2, which means succeeded
                    Assert.Equal(2, JObject.Parse(content)["status"]?.Value<int>());
                }
            }

            Assert.True(hasCompletedJobs);
        }

        private async Task ResetTestContainerAsync()
        {
            if (await _blobContainerClient.ExistsAsync())
            {
                await _blobServiceClient.DeleteBlobContainerAsync(TestConstants.TestContainerName);
            }

            // Make sure the container is deleted before running the tests
            Assert.False(await _blobContainerClient.ExistsAsync());
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
