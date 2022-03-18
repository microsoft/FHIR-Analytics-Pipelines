// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
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

        [Theory]
        [InlineData("https://example.com")]
        [InlineData("https://example.com/")]
        [InlineData("https://example.com/abc")]
        [InlineData("https://example.com/abc/")]
        [InlineData("https://example.com/a/b/c")]
        [InlineData("https://example.com/a/b/c/")]
        public async Task GivenAValidTaskContext_WhenSearchFhirData_CorrectBatchDataShouldBeReturned(string serverUrl)
        {
            var client = CreateDataClient(serverUrl, _mockProvider);

            // First batch
            var searchParameters = new FhirSearchParameters(SampleResourceType, DateTimeOffset.Parse(SampleStartTime), DateTimeOffset.Parse(SampleEndTime), null);
            var bundle1 = await client.SearchAsync(searchParameters);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1), bundle1);

            // Get continuation token
            JObject bundleJObject = JObject.Parse(bundle1);
            var continuationToken = FhirBundleParser.ExtractContinuationToken(bundleJObject);

            // Second batch
            var newSearchParameters = new FhirSearchParameters(SampleResourceType, DateTimeOffset.Parse(SampleStartTime), DateTimeOffset.Parse(SampleEndTime), continuationToken);
            var bundle2 = await client.SearchAsync(newSearchParameters);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile2), bundle2);
        }

        [Fact]
        public async Task GivenAValidTaskContext_WhenSearchDataSourceFailed_ExceptionShouldBeThrown()
        {
            // A different start time will result in an unknown url to mock http handler.
            // An HttpRequestException will throw during search.
            var client = CreateDataClient("https://example.com", _mockProvider);
            var searchParameters = new FhirSearchParameters(SampleResourceType, DateTimeOffset.Parse("2021-07-07T12:00:00+08:00"), DateTimeOffset.Parse(SampleEndTime), string.Empty);

            var exception = await Assert.ThrowsAsync<FhirSearchException>(() => client.SearchAsync(searchParameters));
            Assert.IsType<HttpRequestException>(exception.InnerException);
        }

        [Fact]
        public async Task GivenAnInvalidTokenProvider_WhenSearchDataSourceFailed_ExceptionShouldBeThrown()
        {
            var client = CreateDataClient("https://example.com", _brokenProvider);
            var searchParameters = new FhirSearchParameters(SampleResourceType, DateTimeOffset.Parse(SampleStartTime), DateTimeOffset.Parse(SampleEndTime), string.Empty);

            await Assert.ThrowsAsync<FhirSearchException>(() => client.SearchAsync(searchParameters));
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
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&_sort=_lastUpdated&ct=Y29udGludWF0aW9udG9rZW4%3d",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile2)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&_sort=_lastUpdated&ct=invalidresponsetest",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.InvalidResponseFile)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&_sort=_lastUpdated&ct=invalidbundletest",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.InvalidBundleFile)));

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
