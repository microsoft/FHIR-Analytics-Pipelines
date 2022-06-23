// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Extensions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.SearchOption;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Api
{
    public class FhirApiDataClientTests
    {
        private readonly AzureAccessTokenProvider _brokenProvider = new AzureAccessTokenProvider(new NullLogger<AzureAccessTokenProvider>());
        private readonly MockAccessTokenProvider _mockProvider = new MockAccessTokenProvider();

        private const string SampleResourceType = "Patient";
        private const string SampleStartTime = "2021-08-01T12:00:00+08:00";
        private const string SampleEndTime = "2021-08-09T12:40:59+08:00";

        private const string FhirServerUri = "https://example.com";

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new FhirApiDataClient(null, null, null, null));
        }

        [Fact]
        public async Task GivenAValidDataClient_WhenGetMetadata_MetadataBundleShouldBeReturned()
        {
            var client = CreateDataClient(FhirServerUri, _mockProvider);

            var metaData = await client.GetMetaDataAsync();

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.MetadataFile), metaData);
        }

        [Theory]
        [InlineData("https://example.com")]
        [InlineData("https://example.com/")]
        [InlineData("https://example.com/abc")]
        [InlineData("https://example.com/abc/")]
        [InlineData("https://example.com/a/b/c")]
        [InlineData("https://example.com/a/b/c/")]
        public async Task GivenAValidSearchOption_WhenSearchFhirData_CorrectBatchDataShouldBeReturned(string serverUrl)
        {
            var client = CreateDataClient(serverUrl, _mockProvider);

            // First batch
            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new (FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse(SampleStartTime).ToInstantString()}"),
                new (FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
            };

            var searchOptions = new BaseSearchOptions(SampleResourceType, queryParameters);
            var bundle1 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1), bundle1);

            // Get continuation token
            JObject bundleJObject = JObject.Parse(bundle1);
            var continuationToken = FhirBundleParser.ExtractContinuationToken(bundleJObject);

            searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, continuationToken));

            var bundle2 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile2), bundle2);
        }

        [Fact]
        public async Task GivenAInvalidUrl_WhenSearchFhirData_ExceptionShouldBeThrown()
        {
            // A different start time will result in an unknown url to mock http handler.
            // An HttpRequestException will throw during search.
            var client = CreateDataClient(FhirServerUri, _mockProvider);
            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new (FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse("2021-07-07T12:00:00+08:00").ToInstantString()}"),
                new (FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
                new (FhirApiConstants.ContinuationKey, string.Empty),
            };
            var searchOptions = new BaseSearchOptions(SampleResourceType, queryParameters);

            var exception = await Assert.ThrowsAsync<FhirSearchException>(() => client.SearchAsync(searchOptions));
            Assert.IsType<HttpRequestException>(exception.InnerException);
        }

        [Fact]
        public async Task GivenAnInvalidTokenProvider_WhenSearchFhirData_ExceptionShouldBeThrown()
        {
            var client = CreateDataClient(FhirServerUri, _brokenProvider);
            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new (FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse(SampleStartTime).ToInstantString()}"),
                new (FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
                new (FhirApiConstants.ContinuationKey, string.Empty),
            };
            var searchOptions = new BaseSearchOptions(SampleResourceType, queryParameters);

            await Assert.ThrowsAsync<FhirSearchException>(() => client.SearchAsync(searchOptions));
        }

        [Fact]
        public async Task GivenAValidResourceIdSearchOption_WhenSearchFhirData_CorrectBatchDataShouldBeReturned()
        {
            var client = CreateDataClient(FhirServerUri, _mockProvider);

            var searchOptions = new ResourceIdSearchOptions("MedicationRequest", "3123", null);
            var bundle1 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1), bundle1);
        }

        [Fact]
        public async Task GivenAValidCompartmentSearchOption_WhenSearchFhirData_CorrectBatchDataShouldBeReturned()
        {
            var client = CreateDataClient(FhirServerUri, _mockProvider);
            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new (FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse(SampleStartTime).ToInstantString()}"),
                new (FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
            };
            var searchOptions = new CompartmentSearchOptions("Patient", "347", "*", queryParameters);
            var bundle1 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1), bundle1);
        }

        private FhirApiDataClient CreateDataClient(string fhirServerUrl, IAccessTokenProvider accessTokenProvider)
        {
            // Set up http client.
            var comparer = StringComparer.OrdinalIgnoreCase;
            var requestMap = new Dictionary<string, HttpResponseMessage>(comparer);
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&_sort=_lastUpdated",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&ct=Y29udGludWF0aW9udG9rZW4%3d&_count=1000&_sort=_lastUpdated",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile2)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&ct=invalidresponsetest&_count=1000&_sort=_lastUpdated",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.InvalidResponseFile)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&ct=invalidbundletest&_count=1000&_sort=_lastUpdated",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.InvalidBundleFile)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/MedicationRequest?_id=3123&_count=1000&_sort=_lastUpdated",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/*?_lastUpdated=ge2021-08-01T12%3a00%3a00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3a40%3a59%2b08%3a00&_count=1000&_sort=_lastUpdated",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/metadata",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.MetadataFile)));
            var fhirServerConfig = new FhirServerConfiguration();
            fhirServerConfig.ServerUrl = fhirServerUrl;
            fhirServerConfig.Authentication = AuthenticationType.ManagedIdentity;
            var fhirServerOption = Options.Create(fhirServerConfig);
            var dataSource = new FhirApiDataSource(fhirServerOption);

            var httpClient = new HttpClient(new MockHttpMessageHandler(requestMap));
            ILogger<FhirApiDataClient> logger = new NullLogger<FhirApiDataClient>();

            var dataClient = new FhirApiDataClient(dataSource, httpClient, accessTokenProvider, logger);
            return dataClient;
        }

        private HttpResponseMessage CreateResponseMessage(
            string body,
            HttpStatusCode code = HttpStatusCode.OK)
        {
            var response = new HttpResponseMessage(code);
            response.Content = new StringContent(body);
            return response;
        }
    }
}
