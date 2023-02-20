// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Identity;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common;
using Microsoft.Health.AnalyticsConnector.Common.Authentication;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.DataClient.Api;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Dicom;
using Microsoft.Health.AnalyticsConnector.DataClient.Exceptions;
using Microsoft.Health.AnalyticsConnector.DataClient.Models.DicomApiOption;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.DataClient.UnitTests.Api.Dicom
{
    public class DicomApiDataClientTests
    {
        private static readonly IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private readonly MockTokenCredentialProvider _mockTokenCredentialProvider = new MockTokenCredentialProvider()
        {
            Audience = "https://dicom.healthcareapis.azure.com",
        };

        private readonly NullLogger<DicomApiDataClient> _nullDicomApiDataClientLogger = NullLogger<DicomApiDataClient>.Instance;

        private const string SampleOffset = "1262";
        private const string SampleLimit = "3";
        private const string SampleIncludeMetadata = "true";

        private const string DicomServerUri = "https://example.com/";

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new DicomApiDataClient(null, null, null, null, null));

            var dataSourceConfiguration = new DataSourceConfiguration
            {
                Type = DataSourceType.DICOM,
                DicomServer = new DicomServerConfiguration
                {
                    ServerUrl = DicomServerUri,
                },
            };

            var dataSource = new ApiDataSource(Options.Create(dataSourceConfiguration));

            var httpClient = new HttpClient(new MockHttpMessageHandler(new Dictionary<string, HttpResponseMessage>()));

            Assert.Throws<ArgumentNullException>(
                () => new DicomApiDataClient(null, httpClient, _mockTokenCredentialProvider, _diagnosticLogger, _nullDicomApiDataClientLogger));

            Assert.Throws<ArgumentNullException>(
                () => new DicomApiDataClient(dataSource, null, _mockTokenCredentialProvider, _diagnosticLogger, _nullDicomApiDataClientLogger));

            Assert.Throws<ArgumentNullException>(
                () => new DicomApiDataClient(dataSource, httpClient, null, _diagnosticLogger, _nullDicomApiDataClientLogger));

            Assert.Throws<ArgumentNullException>(
                () => new DicomApiDataClient(dataSource, httpClient, _mockTokenCredentialProvider, _diagnosticLogger, null));
        }

        [Fact]
        public void GivenHttpClientWithAcceptHeader_WhenInitialize_NoExceptionShouldBeThrown()
        {
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                Type = DataSourceType.DICOM,
                DicomServer = new DicomServerConfiguration
                {
                    ServerUrl = DicomServerUri,
                },
            };

            var dataSource = new ApiDataSource(Options.Create(dataSourceConfiguration));

            var httpClient = new HttpClient(new MockHttpMessageHandler(new Dictionary<string, HttpResponseMessage>()));
            httpClient.DefaultRequestHeaders.Add("Accept", "application/json");

            Assert.Null(Record.Exception(() => new DicomApiDataClient(dataSource, httpClient, _mockTokenCredentialProvider, _diagnosticLogger, _nullDicomApiDataClientLogger)));

            httpClient = new HttpClient(new MockHttpMessageHandler(new Dictionary<string, HttpResponseMessage>()));
            httpClient.DefaultRequestHeaders.Add("Accept", "application/dicom+json");

            Assert.Null(Record.Exception(() => new DicomApiDataClient(dataSource, httpClient, _mockTokenCredentialProvider, _diagnosticLogger, _nullDicomApiDataClientLogger)));
        }

        [Fact]
        public async Task GivenAValidDataClient_WhenSearchLatestChangeFeedAsync_LatestChangeFeedShouldBeReturned()
        {
            DicomApiDataClient client = CreateDataClient(DicomServerUri, _mockTokenCredentialProvider);

            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(DicomApiConstants.IncludeMetadataKey, SampleIncludeMetadata),
            };

            var changeFeedLatestOptions = new ChangeFeedLatestOptions(queryParameters);
            string changeFeed = await client.SearchAsync(changeFeedLatestOptions);

            Assert.Equal(TestDataProvider.GetDataFromFile(TestDataConstants.LatestChangeFeedFile1), changeFeed);
        }

        [Fact]
        public async Task GivenAValidDataClient_WhenSearchChangeFeedsAsync_ChangeFeedsShouldBeReturned()
        {
            DicomApiDataClient client = CreateDataClient(DicomServerUri, _mockTokenCredentialProvider);

            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(DicomApiConstants.OffsetKey, SampleOffset),
                new KeyValuePair<string, string>(DicomApiConstants.LimitKey, SampleLimit),
                new KeyValuePair<string, string>(DicomApiConstants.IncludeMetadataKey, SampleIncludeMetadata),
            };

            var changeFeedOffsetOptions = new ChangeFeedOffsetOptions(queryParameters);
            string changeFeeds = await client.SearchAsync(changeFeedOffsetOptions);

            Assert.Equal(TestDataProvider.GetDataFromFile(TestDataConstants.ChangeFeedsFile), changeFeeds);
        }

        [Fact]
        public void GivenAValidDataClient_WhenSearchLatestChangeFeed_ExceptionShouldBeThrown()
        {
            DicomApiDataClient client = CreateDataClient(DicomServerUri, _mockTokenCredentialProvider);

            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(DicomApiConstants.IncludeMetadataKey, SampleIncludeMetadata),
            };

            var changeFeedLatestOptions = new ChangeFeedLatestOptions(queryParameters);

            var exception = Assert.Throws<ApiSearchException>(() => client.Search(changeFeedLatestOptions));
            Assert.Equal("Synchronous search is not supported in DICOM.", exception.Message);
        }

        [Fact]
        public void GivenAValidDataClient_WhenSearchChangeFeeds_ExceptionShouldBeThrown()
        {
            DicomApiDataClient client = CreateDataClient(DicomServerUri, _mockTokenCredentialProvider);

            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(DicomApiConstants.OffsetKey, SampleOffset),
                new KeyValuePair<string, string>(DicomApiConstants.LimitKey, SampleLimit),
                new KeyValuePair<string, string>(DicomApiConstants.IncludeMetadataKey, SampleIncludeMetadata),
            };

            var changeFeedOffsetOptions = new ChangeFeedOffsetOptions(queryParameters);

            var exception = Assert.Throws<ApiSearchException>(() => client.Search(changeFeedOffsetOptions));
            Assert.Equal("Synchronous search is not supported in DICOM.", exception.Message);
        }

        [Fact]
        public async Task GivenNullQueryParameters_WhenSearchAsync_CorrectBatchDataShouldBeReturned()
        {
            DicomApiDataClient client = CreateDataClient(DicomServerUri, _mockTokenCredentialProvider);

            // changefeed/latest
            var changeFeedLatestOptions = new ChangeFeedLatestOptions(null);
            string changeFeed = await client.SearchAsync(changeFeedLatestOptions);

            Assert.Equal(TestDataProvider.GetDataFromFile(TestDataConstants.LatestChangeFeedFile1), changeFeed);

            // changefeed
            var changeFeedOffsetOptions = new ChangeFeedOffsetOptions(null);
            string changeFeeds = await client.SearchAsync(changeFeedOffsetOptions);

            Assert.Equal(TestDataProvider.GetDataFromFile(TestDataConstants.ChangeFeedsFile), changeFeeds);
        }

        [Fact]
        public async Task GivenInvalidServerEnvironmentGroup_WhenSearch_ExceptionShouldBeThrown()
        {
            var testCredentialProvider = new MockTokenCredentialProvider()
            {
                Audience = "TESTINVALIDAUDIENCE",
            };

            DicomApiDataClient client = CreateDataClient(DicomServerUri, testCredentialProvider);
            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(DicomApiConstants.IncludeMetadataKey, SampleIncludeMetadata),
            };

            var changeFeedLatestOptions = new ChangeFeedLatestOptions(queryParameters);
            var exception = await Assert.ThrowsAsync<ApiSearchException>(() => client.SearchAsync(changeFeedLatestOptions));
            Assert.IsType<AuthenticationFailedException>(exception.InnerException);
        }

        [Fact]
        public async Task GivenAnInvalidUrl_WhenSearchAsync_ExceptionShouldBeThrown()
        {
            // A different offset will result in an unknown url to mock http handler.
            // An HttpRequestException will throw during search.
            DicomApiDataClient client = CreateDataClient(DicomServerUri, _mockTokenCredentialProvider);
            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(DicomApiConstants.OffsetKey, "4321"),
                new KeyValuePair<string, string>(DicomApiConstants.LimitKey, SampleLimit),
                new KeyValuePair<string, string>(DicomApiConstants.IncludeMetadataKey, SampleIncludeMetadata),
            };

            var changeFeedLatestOptions = new ChangeFeedLatestOptions(queryParameters);

            var exception = await Assert.ThrowsAsync<ApiSearchException>(() => client.SearchAsync(changeFeedLatestOptions));
            Assert.IsType<HttpRequestException>(exception.InnerException);
        }

        [Theory]
        [InlineData(HttpStatusCode.Unauthorized)]
        [InlineData(HttpStatusCode.NotFound)]
        [InlineData(HttpStatusCode.Forbidden)]
        [InlineData(HttpStatusCode.BadRequest)]
        public async Task GivenAValidDataClient_WhenSearchAndEncounterHttpRequestException_ApiSearchExceptionShouldBeThrown(HttpStatusCode statusCode)
        {
            var messageHandler = new MockHttpMessageHandler(new Dictionary<string, HttpResponseMessage>(), statusCode);
            var httpClient = new HttpClient(messageHandler);

            var dataSource = CreateDicomApiDataSource(DicomServerUri, AuthenticationType.None);
            var searchOptions = new ChangeFeedLatestOptions(null);

            var client = new DicomApiDataClient(dataSource, httpClient, _mockTokenCredentialProvider, _diagnosticLogger, _nullDicomApiDataClientLogger);

            var exception = await Assert.ThrowsAsync<ApiSearchException>(() => client.SearchAsync(searchOptions));
            Assert.IsType<HttpRequestException>(exception.InnerException);
        }

        [Fact]
        public void GivenAValidDataClient_WhenSearchSync_ApiSearchExceptionShouldBeThrown()
        {
            var messageHandler = new MockHttpMessageHandler(new Dictionary<string, HttpResponseMessage>());
            var httpClient = new HttpClient(messageHandler);

            var dataSource = CreateDicomApiDataSource(DicomServerUri, AuthenticationType.None);
            var searchOptions = new ChangeFeedLatestOptions(null);

            var client = new DicomApiDataClient(dataSource, httpClient, _mockTokenCredentialProvider, _diagnosticLogger, _nullDicomApiDataClientLogger);

            var exception = Assert.Throws<ApiSearchException>(() => client.Search(searchOptions));
            Assert.Null(exception.InnerException);
        }

        private DicomApiDataClient CreateDataClient(string dicomServerUrl, ITokenCredentialProvider mockProvider, IApiDataSource dataSource = null)
        {
            // Set up http client.
            StringComparer comparer = StringComparer.OrdinalIgnoreCase;
            Dictionary<string, HttpResponseMessage> requestMap = new (comparer)
            {
                {
                    $"{dicomServerUrl.TrimEnd('/')}/v1/{DicomApiConstants.ChangeFeedKey}/{DicomApiConstants.LatestKey}",
                    CreateResponseMessage(TestDataProvider.GetDataFromFile(TestDataConstants.LatestChangeFeedFile1))
                },
                {
                    $"{dicomServerUrl.TrimEnd('/')}/v1/{DicomApiConstants.ChangeFeedKey}/{DicomApiConstants.LatestKey}?{DicomApiConstants.IncludeMetadataKey}={SampleIncludeMetadata}",
                    CreateResponseMessage(TestDataProvider.GetDataFromFile(TestDataConstants.LatestChangeFeedFile1))
                },
                {
                    $"{dicomServerUrl.TrimEnd('/')}/v1/{DicomApiConstants.ChangeFeedKey}",
                    CreateResponseMessage(TestDataProvider.GetDataFromFile(TestDataConstants.ChangeFeedsFile))
                },
                {
                    $"{dicomServerUrl.TrimEnd('/')}/v1/{DicomApiConstants.ChangeFeedKey}?{DicomApiConstants.OffsetKey}={SampleOffset}&{DicomApiConstants.LimitKey}={SampleLimit}&{DicomApiConstants.IncludeMetadataKey}={SampleIncludeMetadata}",
                    CreateResponseMessage(TestDataProvider.GetDataFromFile(TestDataConstants.ChangeFeedsFile))
                },
            };

            dataSource ??= CreateDicomApiDataSource(dicomServerUrl, AuthenticationType.ManagedIdentity);

            var httpClient = new HttpClient(new MockHttpMessageHandler(requestMap));

            var dataClient = new DicomApiDataClient(dataSource, httpClient, mockProvider, _diagnosticLogger, _nullDicomApiDataClientLogger);

            return dataClient;
        }

        private static IApiDataSource CreateDicomApiDataSource(string dicomServerUrl, AuthenticationType authenticationType)
        {
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                Type = DataSourceType.DICOM,
                DicomServer = new DicomServerConfiguration
                {
                    ServerUrl = dicomServerUrl,
                    Authentication = authenticationType,
                },
            };

            var dataSource = new ApiDataSource(Options.Create(dataSourceConfiguration));

            return dataSource;
        }

        private static HttpResponseMessage CreateResponseMessage(
            string body,
            HttpStatusCode code = HttpStatusCode.OK)
        {
            var response = new HttpResponseMessage(code)
            {
                Content = new StringContent(body),
            };
            return response;
        }
    }
}
