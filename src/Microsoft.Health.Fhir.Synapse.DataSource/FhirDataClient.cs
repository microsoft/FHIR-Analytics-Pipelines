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
using Microsoft.Health.Fhir.Synapse.Azure;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.DataSerialization;
using Microsoft.Health.Fhir.Synapse.DataSource.Api;
using Microsoft.Health.Fhir.Synapse.DataSource.Bundle;
using Microsoft.Health.Fhir.Synapse.DataSource.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataSource.Extensions;

namespace Microsoft.Health.Fhir.Synapse.DataSource
{
    public sealed class FhirDataClient : IFhirDataClient
    {
        private readonly IFhirDataSource _dataSource;
        private readonly HttpClient _httpClient;
        private readonly IAccessTokenProvider _accessTokenProvider;
        private readonly IFhirSerializer _fhirSerializer;
        private readonly ILogger<FhirDataClient> _logger;

        public FhirDataClient(
            IFhirDataSource dataSource,
            HttpClient httpClient,
            IAccessTokenProvider accessTokenProvider,
            IFhirSerializer fhirSerializer,
            ILogger<FhirDataClient> logger)
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

        public async Task<FhirElementBatchData> GetAsync(TaskContext context, CancellationToken cancellationToken = default)
        {
            return await SearchAsync(context, cancellationToken);
        }

        private async Task<FhirElementBatchData> SearchAsync(
            TaskContext context,
            CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }

            var searchUri = CreateSearchUri(context);
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
        /// <param name="context">Task context.</param>
        /// <returns>Uri with search parameters.</returns>
        private Uri CreateSearchUri(TaskContext context)
        {
            var baseUri = new Uri(_dataSource.FhirServerUrl);
            var uri = new Uri(baseUri, context.ResourceType);

            var parameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{context.StartTime.ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{context.EndTime.ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiConstants.PageCount.ToString()),
                new KeyValuePair<string, string>(FhirApiConstants.SortKey, FhirApiConstants.LastUpdatedKey),
            };

            if (!string.IsNullOrEmpty(context.ContinuationToken))
            {
                parameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, context.ContinuationToken));
            }

            return uri.AddQueryString(parameters);
        }
    }
}
