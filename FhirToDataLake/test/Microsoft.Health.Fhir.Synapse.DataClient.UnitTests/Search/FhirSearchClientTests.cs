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
using Microsoft.Health.Fhir.Synapse.Azure.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Fhir;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Search
{
    public class FhirSearchClientTests
    {
        private readonly AzureAccessTokenProvider _brokenProvider = new AzureAccessTokenProvider(new NullLogger<AzureAccessTokenProvider>());
        private readonly MockAccessTokenProvider _mockProvider = new MockAccessTokenProvider();

        private const string SampleResourceType = "Patient";
        private const string SampleStartTime = "2021-08-01T12:00:00+08:00";
        private const string SampleEndTime = "2021-08-09T12:40:59+08:00";
        private const string SampleContinuationToken = "Y29udGludWF0aW9udG9rZW4=";

        [Fact]
        public async Task GivenAValidTaskContext_WhenSearchFhirData_CorrectBatchDataShouldBeReturned()
        {
            var client = CreateDataClient(_mockProvider);

            // First batch
            var batchData = await client.GetAsync(SampleResourceType, DateTimeOffset.Parse(SampleStartTime), DateTimeOffset.Parse(SampleEndTime), null);

            Assert.Equal(2, batchData.Values.Count());
            Assert.Equal(SampleContinuationToken, batchData.ContinuationToken);

            // Second batch
            var newBatchData = await client.GetAsync(SampleResourceType, DateTimeOffset.Parse(SampleStartTime), DateTimeOffset.Parse(SampleEndTime), batchData.ContinuationToken);

            Assert.Single(newBatchData.Values);
            Assert.Null(newBatchData.ContinuationToken);
        }

        [Fact]
        public async Task GivenAValidTaskContext_WhenSearchDataSourceFailed_ExceptionShouldBeThrown()
        {
            // A different start time will result in an unknown url to mock http handler.
            // An HttpRequestException will throw during search.
            var client = CreateDataClient(_mockProvider);

            var exception = await Assert.ThrowsAsync<FhirSearchException>(() => client.GetAsync(SampleResourceType, DateTimeOffset.Parse("2021-07-07T12:00:00+08:00"), DateTimeOffset.Parse(SampleEndTime), string.Empty));
            Assert.IsType<HttpRequestException>(exception.InnerException);
        }

        [Fact]
        public async Task GivenAnInvalidTokenProvider_WhenSearchDataSourceFailed_ExceptionShouldBeThrown()
        {
            var client = CreateDataClient(_brokenProvider);

            await Assert.ThrowsAsync<FhirSearchException>(() => client.GetAsync(SampleResourceType, DateTimeOffset.Parse(SampleStartTime), DateTimeOffset.Parse(SampleEndTime), string.Empty));
        }

        [Theory]
        [InlineData("invalidresponsetest")]
        [InlineData("invalidbundletest")]
        public async Task GivenAValidSearchInput_WhenSearchDataSource_AndGetInvalidResponse_ExceptionShouldBeThrown(string invalidCt)
        {
            var client = CreateDataClient(_mockProvider);

            await Assert.ThrowsAsync<FhirBundleParseException>(() => client.GetAsync(SampleResourceType, DateTimeOffset.Parse(SampleStartTime), DateTimeOffset.Parse(SampleEndTime), invalidCt));
        }

        private FhirApiDataClient CreateDataClient(IAccessTokenProvider accessTokenProvider)
        {
            // Set up http client.
            var comparer = StringComparer.OrdinalIgnoreCase;
            var requestMap = new Dictionary<string, HttpResponseMessage>(comparer);
            requestMap.Add(
                "https://example.com/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&_sort=_lastUpdated",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1)));
            requestMap.Add(
                "https://example.com/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&_sort=_lastUpdated&ct=Y29udGludWF0aW9udG9rZW4%3d",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile2)));
            requestMap.Add(
                "https://example.com/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&_sort=_lastUpdated&ct=invalidresponsetest",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.InvalidResponseFile)));
            requestMap.Add(
                "https://example.com/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&_sort=_lastUpdated&ct=invalidbundletest",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.InvalidBundleFile)));

            var fhirServerConfig = new FhirServerConfiguration();
            fhirServerConfig.ServerUrl = "https://example.com";
            fhirServerConfig.Authentication = AuthenticationType.ManagedIdentity;
            var fhirServerOption = Options.Create(fhirServerConfig);
            var dataSource = new FhirApiDataSource(fhirServerOption);

            var httpClient = new HttpClient(new MockHttpMessageHandler(requestMap));
            ILogger<FhirApiDataClient> logger = new NullLogger<FhirApiDataClient>();

            var dataClient = new FhirApiDataClient(dataSource, httpClient, accessTokenProvider, new FhirSerializer(fhirServerOption), logger);
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
