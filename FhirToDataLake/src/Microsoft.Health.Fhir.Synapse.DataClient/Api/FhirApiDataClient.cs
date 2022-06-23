// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Extensions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.SearchOption;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public sealed class FhirApiDataClient : IFhirDataClient
    {
        private const string MetadataKey = "metadata";
        private readonly IFhirApiDataSource _dataSource;
        private readonly HttpClient _httpClient;
        private readonly IAccessTokenProvider _accessTokenProvider;
        private readonly ILogger<FhirApiDataClient> _logger;

        public FhirApiDataClient(
            IFhirApiDataSource dataSource,
            HttpClient httpClient,
            IAccessTokenProvider accessTokenProvider,
            ILogger<FhirApiDataClient> logger)
        {
            EnsureArg.IsNotNull(dataSource, nameof(dataSource));
            EnsureArg.IsNotNullOrEmpty(dataSource.FhirServerUrl, nameof(dataSource.FhirServerUrl));
            EnsureArg.IsNotNull(httpClient, nameof(httpClient));
            EnsureArg.IsNotNull(accessTokenProvider, nameof(accessTokenProvider));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataSource = dataSource;
            _httpClient = httpClient;
            _accessTokenProvider = accessTokenProvider;
            _logger = logger;

            // Timeout will be handled by Polly policy.
            _httpClient.Timeout = Timeout.InfiniteTimeSpan;
        }

        public async Task<string> GetMetaDataAsync(CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }

            var baseUri = new Uri(_dataSource.FhirServerUrl);

            var uri = new Uri(baseUri, MetadataKey);

            return await GetResponseFromHttpRequestAsync(uri, null, cancellationToken);
        }

        public async Task<string> SearchAsync(
            BaseSearchOptions fhirSearchOptions,
            CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }

            var searchUri = CreateSearchUri(fhirSearchOptions);

            string accessToken = null;
            try
            {
                if (_dataSource.Authentication == AuthenticationType.ManagedIdentity)
                {
                    // Currently we support accessing FHIR server endpoints with Managed Identity.
                    // Obtaining access token against a resource uri only works with Azure API for FHIR now.
                    // To do: add configuration for OSS FHIR server endpoints.

                    // The thread-safe AzureServiceTokenProvider class caches the token in memory and retrieves it from Azure AD just before expiration.
                    // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/service-to-service-authentication#using-the-library
                    accessToken = await _accessTokenProvider.GetAccessTokenAsync(_dataSource.FhirServerUrl, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Get fhir server access token failed, Reason: '{reason}'", ex);
                throw new FhirSearchException("Get fhir server access token failed", ex);
            }

            return await GetResponseFromHttpRequestAsync(searchUri, accessToken, cancellationToken);
        }

        private Uri CreateSearchUri(BaseSearchOptions fhirSearchOptions)
        {
            // If the baseUri has relative parts (like /api), then the relative part must be terminated with a slash (like /api/).
            // Otherwise the relative part will be omitted when creating new search Uris. See https://docs.microsoft.com/en-us/dotnet/api/system.uri.-ctor?view=net-6.0
            var serverUrl = _dataSource.FhirServerUrl;
            if (!_dataSource.FhirServerUrl.EndsWith("/"))
            {
                serverUrl = $"{_dataSource.FhirServerUrl}/";
            }

            var baseUri = new Uri(serverUrl);

            var uri = new Uri(baseUri, fhirSearchOptions.RelativeUri());

            uri = uri.AddQueryString(fhirSearchOptions.QueryParameters);

            // add shared parameters _count & sort
            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiConstants.PageCount.ToString()),
                new KeyValuePair<string, string>(FhirApiConstants.SortKey, FhirApiConstants.LastUpdatedKey),
            };

            return uri.AddQueryString(queryParameters);
        }

        private async Task<string> GetResponseFromHttpRequestAsync(Uri uri, string accessToken = null, CancellationToken cancellationToken = default)
        {
            try
            {
                var searchRequest = new HttpRequestMessage(HttpMethod.Get, uri);
                if (accessToken != null)
                {
                    // Currently we support accessing FHIR server endpoints with Managed Identity.
                    // Obtaining access token against a resource uri only works with Azure API for FHIR now.
                    // To do: add configuration for OSS FHIR server endpoints.

                    // The thread-safe AzureServiceTokenProvider class caches the token in memory and retrieves it from Azure AD just before expiration.
                    // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/service-to-service-authentication#using-the-library
                    searchRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                }

                HttpResponseMessage response = await _httpClient.SendAsync(searchRequest, cancellationToken);
                response.EnsureSuccessStatusCode();
                _logger.LogInformation("Successfully retrieved result for url: '{url}'.", uri);

                return await response.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError("Get response from http request failed. Url: '{url}', Reason: '{reason}'", uri, ex);
                throw new FhirSearchException(
                    string.Format(Resource.FhirSearchFailed, uri),
                    ex);
            }
        }
    }
}
