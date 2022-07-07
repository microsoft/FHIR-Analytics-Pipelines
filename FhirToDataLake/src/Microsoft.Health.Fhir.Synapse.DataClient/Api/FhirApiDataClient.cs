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
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public sealed class FhirApiDataClient : IFhirDataClient
    {
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

        public async Task<string> SearchAsync(
            BaseFhirApiOptions fhirApiOptions,
            CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }

            Uri searchUri;
            try
            {
                searchUri = CreateSearchUri(fhirApiOptions);
            }
            catch (Exception ex)
            {
                _logger.LogError("Create search Uri failed, Reason: '{reason}'", ex);
                throw new FhirSearchException("Create search Uri failed", ex);
            }

            string accessToken = null;
            if (fhirApiOptions.IsAccessTokenRequired())
            {
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
            }

            return await GetResponseFromHttpRequestAsync(searchUri, accessToken, cancellationToken);
        }

        private Uri CreateSearchUri(BaseFhirApiOptions fhirApiOptions)
        {
            var serverUrl = _dataSource.FhirServerUrl;

            var baseUri = new Uri(serverUrl);

            var uri = new Uri(baseUri, fhirApiOptions.RelativeUri());

            // the query parameters is null for metadata
            if (fhirApiOptions.QueryParameters == null)
            {
                return uri;
            }

            uri = uri.AddQueryString(fhirApiOptions.QueryParameters);

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
