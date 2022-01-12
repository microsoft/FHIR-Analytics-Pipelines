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
using Microsoft.Health.Fhir.Synapse.DataClient.Fhir;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public sealed class FhirApiDataClient : IFhirDataClient
    {
        private readonly IFhirApiDataSource _dataSource;
        private readonly HttpClient _httpClient;
        private readonly IAccessTokenProvider _accessTokenProvider;
        private readonly IFhirSerializer _fhirSerializer;
        private readonly ILogger<FhirApiDataClient> _logger;

        public FhirApiDataClient(
            IFhirApiDataSource dataSource,
            HttpClient httpClient,
            IAccessTokenProvider accessTokenProvider,
            IFhirSerializer fhirSerializer,
            ILogger<FhirApiDataClient> logger)
        {
            EnsureArg.IsNotNull(dataSource, nameof(dataSource));
            EnsureArg.IsNotNull(httpClient, nameof(httpClient));
            EnsureArg.IsNotNull(accessTokenProvider, nameof(accessTokenProvider));
            EnsureArg.IsNotNull(fhirSerializer, nameof(fhirSerializer));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataSource = dataSource;
            _httpClient = httpClient;
            _accessTokenProvider = accessTokenProvider;
            _fhirSerializer = fhirSerializer;
            _logger = logger;

            // Timeout will be handled by Polly policy.
            _httpClient.Timeout = Timeout.InfiniteTimeSpan;
        }

        public async Task<FhirElementBatchData> GetAsync(
            FhirSearchParameters searchParameters,
            CancellationToken cancellationToken = default)
        {
            return await SearchAsync(searchParameters, cancellationToken);
        }

        private async Task<FhirElementBatchData> SearchAsync(
            FhirSearchParameters searchParameters,
            CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }

            var searchUri = CreateSearchUri(searchParameters);
            HttpResponseMessage response;
            string bundleContent;

            try
            {
                var searchRequest = new HttpRequestMessage(HttpMethod.Get, searchUri);
                if (_dataSource.Authentication == AuthenticationType.ManagedIdentity)
                {
                    // Currently we support accessing FHIR server endpoints with Managed Identity.
                    // Obtaining access token against a resource uri only works with Azure API for FHIR now.
                    // To do: add configuration for OSS FHIR server endpoints.

                    // The thread-safe AzureServiceTokenProvider class caches the token in memory and retrieves it from Azure AD just before expiration.
                    // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/service-to-service-authentication#using-the-library
                    var accessToken = await _accessTokenProvider.GetAccessTokenAsync(_dataSource.FhirServerUrl, cancellationToken);
                    searchRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                }

                response = await _httpClient.SendAsync(searchRequest, cancellationToken);
                response.EnsureSuccessStatusCode();
                _logger.LogInformation("Successfully retrieved search result for url: '{url}'.", searchUri);

                bundleContent = await response.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError("Search FHIR server failed. Url: '{url}', Reason: '{reason}'", searchUri, ex);
                throw new FhirSearchException(
                    string.Format(Resource.FhirSearchFailed, searchUri),
                    ex);
            }

            try
            {
                var bundleElement = _fhirSerializer.DeserializeToElement(bundleContent);
                return FhirBundleParser.ParseBatchData(bundleElement);
            }
            catch (Exception ex)
            {
                _logger.LogError("Parse response bundle failed. Reason: '{reason}'", ex.ToString());
                throw new FhirBundleParseException(
                    string.Format(
                        Resource.FhirBundleParseFailed,
                        ex),
                    ex);
            }
        }

        /// <summary>
        /// Sample uri: http://{FhirServerUrl}/{ResourceType}?_lastUpdated=ge{StartTimestamp}&_lastUpdated=lt{EndTimestamp}&_count={PageCount}&_sort=_lastUpdated&ct={ContinuationToken}.
        /// </summary>
        /// <param name="searchParameters">The FHIR search parameters.</param>
        /// <returns>Uri with search parameters.</returns>
        private Uri CreateSearchUri(FhirSearchParameters searchParameters)
        {
            var baseUri = new Uri(_dataSource.FhirServerUrl);
            var uri = new Uri(baseUri, searchParameters.ResourceType);

            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{searchParameters.StartTime.ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{searchParameters.EndTime.ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiConstants.PageCount.ToString()),
                new KeyValuePair<string, string>(FhirApiConstants.SortKey, FhirApiConstants.LastUpdatedKey),
            };

            if (!string.IsNullOrEmpty(searchParameters.ContinuationToken))
            {
                queryParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, searchParameters.ContinuationToken));
            }

            return uri.AddQueryString(queryParameters);
        }
    }
}
