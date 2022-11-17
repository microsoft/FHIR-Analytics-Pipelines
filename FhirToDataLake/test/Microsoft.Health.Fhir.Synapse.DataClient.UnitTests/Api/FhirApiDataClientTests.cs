// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Extensions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Api
{
    public class FhirApiDataClientTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private readonly MockTokenCredentialProvider _mockTokenCredentialProvider = new MockTokenCredentialProvider();
        private readonly NullLogger<FhirApiDataClient> _nullFhirApiDataClientLogger =
            NullLogger<FhirApiDataClient>.Instance;

        private const string SampleResourceType = "Patient";
        private const string SampleStartTime = "2021-08-01T12:00:00+08:00";
        private const string SampleEndTime = "2021-08-09T12:40:59+08:00";

        private const string FhirServerUri = "https://example.com";

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new FhirApiDataClient(null, null, null, null, null));

            var fhirServerConfiguration = new FhirServerConfiguration()
            {
                ServerUrl = FhirServerUri,
            };

            var dataSource = new FhirApiDataSource(Options.Create(fhirServerConfiguration));

            var httpClient = new HttpClient(new MockHttpMessageHandler(new Dictionary<string, HttpResponseMessage>()));

            Assert.Throws<ArgumentNullException>(
                () => new FhirApiDataClient(null, httpClient, _mockTokenCredentialProvider, _diagnosticLogger, _nullFhirApiDataClientLogger));

            Assert.Throws<ArgumentNullException>(
                () => new FhirApiDataClient(dataSource, null, _mockTokenCredentialProvider, _diagnosticLogger, _nullFhirApiDataClientLogger));

            Assert.Throws<ArgumentNullException>(
                () => new FhirApiDataClient(dataSource, httpClient, null, _diagnosticLogger, _nullFhirApiDataClientLogger));

            Assert.Throws<ArgumentNullException>(
                () => new FhirApiDataClient(dataSource, httpClient, _mockTokenCredentialProvider, _diagnosticLogger, null));
        }

        [Fact]
        public void GivenAValidDataClient_WhenGetMetadataSync_MetadataBundleShouldBeReturned()
        {
            FhirApiDataClient client = CreateDataClient(FhirServerUri, _mockTokenCredentialProvider);

            var metadataOption = new MetadataOptions();
            string metaData = client.Search(metadataOption);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.R4MetadataFile), metaData);
        }

        [Fact]
        public async Task GivenAValidDataClient_WhenGetMetadataASync_MetadataBundleShouldBeReturned()
        {
            FhirApiDataClient client = CreateDataClient(FhirServerUri, _mockTokenCredentialProvider);

            var metadataOption = new MetadataOptions();
            string metaData = await client.SearchAsync(metadataOption);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.R4MetadataFile), metaData);
        }

        [Fact]
        public void GivenSearchOptionRequiredAccessToken_WhenSearchFhirDataSync_ExceptionShouldBeThrown()
        {
            FhirApiDataClient client = CreateDataClient(FhirServerUri, _mockTokenCredentialProvider);
            var searchOptions = new BaseSearchOptions("Patient", null);
            Assert.Throws<FhirSearchException>(() => client.Search(searchOptions));
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
            FhirApiDataClient client = CreateDataClient(serverUrl, _mockTokenCredentialProvider);

            // First batch
            List<KeyValuePair<string, string>> queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse(SampleStartTime).ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Batch.ToString("d")),
            };

            var searchOptions = new BaseSearchOptions(SampleResourceType, queryParameters);
            string bundle1 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1), bundle1);

            // Get continuation token
            JObject bundleJObject = JObject.Parse(bundle1);
            string continuationToken = FhirBundleParser.ExtractContinuationToken(bundleJObject);

            searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, continuationToken));

            string bundle2 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile2), bundle2);
        }

        [Theory]
        [InlineData("https://example.com#")]
        [InlineData("https://example.com#/")]
        [InlineData("https://example.com/#")]
        [InlineData("https://example.com/#/")]
        public async Task GivenServerUrlWithPoundKey_WhenSearchFhirData_CorrectBatchDataShouldBeReturned(string serverUrl)
        {
            IFhirApiDataSource datasource = CreateFhirApiDataSource(serverUrl, AuthenticationType.ManagedIdentity);
            FhirApiDataClient client = CreateDataClient("https://example.com", _mockTokenCredentialProvider, datasource);

            // First batch
            List<KeyValuePair<string, string>> queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse(SampleStartTime).ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Batch.ToString("d")),
            };

            var searchOptions = new BaseSearchOptions(SampleResourceType, queryParameters);
            string bundle1 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1), bundle1);

            // Get continuation token
            JObject bundleJObject = JObject.Parse(bundle1);
            string continuationToken = FhirBundleParser.ExtractContinuationToken(bundleJObject);

            searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, continuationToken));

            string bundle2 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile2), bundle2);
        }

        [Fact]
        public async Task GivenNullQueryParameters_WhenSearchFhirData_CorrectBatchDataShouldBeReturned()
        {
            FhirApiDataClient client = CreateDataClient(FhirServerUri, _mockTokenCredentialProvider);
            var searchOptions = new BaseSearchOptions(SampleResourceType, null);
            string bundle1 = await client.SearchAsync(searchOptions);
            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1), bundle1);
        }

        [Fact]
        public async Task GivenAInvalidUrl_WhenSearchFhirData_ExceptionShouldBeThrown()
        {
            // A different start time will result in an unknown url to mock http handler.
            // An HttpRequestException will throw during search.
            FhirApiDataClient client = CreateDataClient(FhirServerUri, _mockTokenCredentialProvider);
            List<KeyValuePair<string, string>> queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse("2021-07-07T12:00:00+08:00").ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, string.Empty),
            };
            var searchOptions = new BaseSearchOptions(SampleResourceType, queryParameters);

            var exception = await Assert.ThrowsAsync<FhirSearchException>(() => client.SearchAsync(searchOptions));
            Assert.IsType<HttpRequestException>(exception.InnerException);
        }

        [Fact]
        public async Task GivenAnInvalidTokenProvider_WhenSearchFhirData_ExceptionShouldBeThrown()
        {
            FhirApiDataClient client = CreateDataClient(FhirServerUri, _mockTokenCredentialProvider);
            List<KeyValuePair<string, string>> queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse(SampleStartTime).ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, string.Empty),
            };
            var searchOptions = new BaseSearchOptions(SampleResourceType, queryParameters);

            await Assert.ThrowsAsync<FhirSearchException>(() => client.SearchAsync(searchOptions));
        }

        [Fact]
        public async Task GivenAValidResourceIdSearchOption_WhenSearchFhirData_CorrectBatchDataShouldBeReturned()
        {
            FhirApiDataClient client = CreateDataClient(FhirServerUri, _mockTokenCredentialProvider);

            var searchOptions = new ResourceIdSearchOptions("MedicationRequest", "3123", null);
            string bundle1 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1), bundle1);
        }

        [Fact]
        public async Task GivenAValidCompartmentSearchOption_WhenSearchFhirData_CorrectBatchDataShouldBeReturned()
        {
            FhirApiDataClient client = CreateDataClient(FhirServerUri, _mockTokenCredentialProvider);
            List<KeyValuePair<string, string>> queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse(SampleStartTime).ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Batch.ToString("d")),
            };
            var searchOptions = new CompartmentSearchOptions("Patient", "347", "*", queryParameters);
            string bundle1 = await client.SearchAsync(searchOptions);

            Assert.Equal(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1), bundle1);
        }

        private FhirApiDataClient CreateDataClient(string fhirServerUrl, ITokenCredentialProvider mockProvider, IFhirApiDataSource dataSource = null)
        {
            // Set up http client.
            StringComparer comparer = StringComparer.OrdinalIgnoreCase;
            Dictionary<string, HttpResponseMessage> requestMap = new Dictionary<string, HttpResponseMessage>(comparer);
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&ct=Y29udGludWF0aW9udG9rZW4%3d",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile2)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&ct=invalidresponsetest",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.InvalidResponseFile)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_lastUpdated=ge2021-08-01T12%3A00%3A00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3A40%3A59%2b08%3a00&_count=1000&ct=invalidbundletest",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.InvalidBundleFile)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient?_count=1000",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/MedicationRequest?_id=3123",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient/347/*?_lastUpdated=ge2021-08-01T12%3a00%3a00%2b08%3a00&_lastUpdated=lt2021-08-09T12%3a40%3a59%2b08%3a00&_count=1000",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/metadata",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.R4MetadataFile)));
            requestMap.Add(
                $"{fhirServerUrl.TrimEnd('/')}/Patient",
                CreateResponseMessage(TestDataProvider.GetBundleFromFile(TestDataConstants.BundleFile1)));

            dataSource ??= CreateFhirApiDataSource(fhirServerUrl, AuthenticationType.ManagedIdentity);

            var httpClient = new HttpClient(new MockHttpMessageHandler(requestMap));

            var dataClient = new FhirApiDataClient(dataSource, httpClient, mockProvider, _diagnosticLogger, _nullFhirApiDataClientLogger);
            return dataClient;
        }

        private IFhirApiDataSource CreateFhirApiDataSource(string fhirServerUrl, AuthenticationType authenticationType)
        {
            var fhirServerConfig = new FhirServerConfiguration
            {
                ServerUrl = fhirServerUrl,
                Authentication = authenticationType,
            };

            IOptions<FhirServerConfiguration> fhirServerOption = Options.Create(fhirServerConfig);
            var dataSource = new FhirApiDataSource(fhirServerOption);

            return dataSource;
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
