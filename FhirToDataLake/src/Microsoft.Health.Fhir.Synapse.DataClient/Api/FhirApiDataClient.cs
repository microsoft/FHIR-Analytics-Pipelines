// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Extensions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Polly.CircuitBreaker;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public sealed class FhirApiDataClient : IFhirDataClient
    {
        private readonly IFhirApiDataSource _dataSource;
        private readonly HttpClient _httpClient;
        private readonly IAccessTokenProvider _accessTokenProvider;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<FhirApiDataClient> _logger;

        private const int RetryCount = 1;
        private const int RetryTimeSpan = 5000;

        public FhirApiDataClient(
            IFhirApiDataSource dataSource,
            HttpClient httpClient,
            ITokenCredentialProvider tokenCredentialProvider,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirApiDataClient> logger)
        {
            EnsureArg.IsNotNull(dataSource, nameof(dataSource));
            EnsureArg.IsNotNullOrEmpty(dataSource.FhirServerUrl, nameof(dataSource.FhirServerUrl));
            EnsureArg.IsNotNull(httpClient, nameof(httpClient));
            EnsureArg.IsNotNull(tokenCredentialProvider, nameof(tokenCredentialProvider));
            EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataSource = dataSource;
            _httpClient = httpClient;
            _accessTokenProvider = new AzureAccessTokenProvider(
                tokenCredentialProvider.GetCredential(TokenCredentialTypes.External),
                diagnosticLogger,
                new Logger<AzureAccessTokenProvider>(new LoggerFactory()));
            _diagnosticLogger = diagnosticLogger;
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
                _diagnosticLogger.LogError(string.Format("Create search Uri failed, Reason: '{reason}'", ex.Message));
                _logger.LogInformation(ex, "Create search Uri failed, Reason: '{reason}'", ex.Message);
                throw new FhirSearchException("Create search Uri failed", ex);
            }

            string accessToken = null;
            if (fhirApiOptions.IsAccessTokenRequired)
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
                    _diagnosticLogger.LogError(string.Format("Get fhir server access token failed, Reason: '{reason}'", ex.Message));
                    _logger.LogInformation(ex, "Get fhir server access token failed, Reason: '{reason}'", ex.Message);
                    throw new FhirSearchException("Get fhir server access token failed", ex);
                }
            }

            return await GetResponseFromHttpRequestAsync(searchUri, accessToken, cancellationToken);
        }

        public string Search(BaseFhirApiOptions fhirApiOptions)
        {
            if (fhirApiOptions.IsAccessTokenRequired && _dataSource.Authentication == AuthenticationType.ManagedIdentity)
            {
                _diagnosticLogger.LogError("Synchronous search doesn't support AccessToken, please use Asynchronous method SearchAsync() instead.");
                _logger.LogInformation("Synchronous search doesn't support AccessToken, please use Asynchronous method SearchAsync() instead.");
                throw new FhirSearchException(
                    "Synchronous search doesn't support AccessToken, please use Asynchronous method SearchAsync() instead.");
            }

            Uri searchUri;
            try
            {
                searchUri = CreateSearchUri(fhirApiOptions);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Create search Uri failed, Reason: '{ex.Message}'");
                _logger.LogInformation(ex, "Create search Uri failed, Reason: '{reason}'", ex.Message);
                throw new FhirSearchException("Create search Uri failed", ex);
            }

            return GetResponseFromHttpRequest(searchUri);
        }

        private Uri CreateSearchUri(BaseFhirApiOptions fhirApiOptions)
        {
            string serverUrl = _dataSource.FhirServerUrl;

            Uri baseUri = new Uri(serverUrl);

            Uri uri = new Uri(baseUri, fhirApiOptions.RelativeUri());

            // the query parameters is null for metadata
            if (fhirApiOptions.QueryParameters == null)
            {
                return uri;
            }

            uri = uri.AddQueryString(fhirApiOptions.QueryParameters);

            // add shared parameters _count
            List<KeyValuePair<string, string>> queryParameters = new List<KeyValuePair<string, string>>
            {
                new (FhirApiConstants.PageCountKey, FhirApiConstants.PageCount.ToString()),
            };

            return uri.AddQueryString(queryParameters);
        }

        private async Task<string> GetResponseFromHttpRequestAsync(Uri uri, string accessToken = null, CancellationToken cancellationToken = default)
        {
            try
            {
                HttpRequestMessage searchRequest = new HttpRequestMessage(HttpMethod.Get, uri);
                if (accessToken != null)
                {
                    // Currently we support accessing FHIR server endpoints with Managed Identity.
                    // Obtaining access token against a resource uri only works with Azure API for FHIR now.
                    // To do: add configuration for OSS FHIR server endpoints.

                    // The thread-safe AzureServiceTokenProvider class caches the token in memory and retrieves it from Azure AD just before expiration.
                    // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/service-to-service-authentication#using-the-library
                    searchRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                }

                int retryCount = 0;
                bool retry = true;
                HttpResponseMessage response = await _httpClient.SendAsync(searchRequest, cancellationToken);
                while (retry)
                {
                    // retry for 429 exception
                    if (retryCount < RetryCount && response.StatusCode == HttpStatusCode.TooManyRequests)
                    {
                        _logger.LogInformation("Get response from http request failed due to 429 too many requests, will delay for {0}ms and retry it. Url: '{1}',", RetryTimeSpan, uri);
                        Thread.Sleep(RetryTimeSpan);
                        response = await _httpClient.SendAsync(searchRequest, cancellationToken);
                        retryCount++;
                    }
                    else
                    {
                        retry = false;
                    }
                }

                response.EnsureSuccessStatusCode();

                _logger.LogInformation("Successfully retrieved result for url: '{url}'.", uri);

                return await response.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (HttpRequestException hrEx)
            {
                switch (hrEx.StatusCode)
                {
                    case HttpStatusCode.Unauthorized:
                        _diagnosticLogger.LogError(string.Format("Failed to search from FHIR server: FHIR server {0} is unauthorized.", _dataSource.FhirServerUrl));
                        _logger.LogInformation(hrEx, "Failed to search from FHIR server: FHIR server {0} is unauthorized.", _dataSource.FhirServerUrl);
                        break;
                    case HttpStatusCode.NotFound:
                        _diagnosticLogger.LogError(string.Format("Failed to search from FHIR server: FHIR server {0} is not found.", _dataSource.FhirServerUrl));
                        _logger.LogInformation(hrEx, "Failed to search from FHIR server: FHIR server {0} is not found.", _dataSource.FhirServerUrl);
                        break;
                    default:
                        _diagnosticLogger.LogError(string.Format("Failed to search from FHIR server: Status code: {0}.", hrEx.StatusCode));
                        _logger.LogInformation(hrEx, "Failed to search from FHIR server: Status code: {0}.", hrEx.StatusCode);
                        break;
                }

                throw new FhirSearchException(
                    string.Format(Resource.FhirSearchFailed, uri),
                    hrEx);
            }
            catch (BrokenCircuitException bcEx)
            {
                _diagnosticLogger.LogError($"Failed to search from FHIR server. Reason: {bcEx.Message}");
                _logger.LogInformation(bcEx, "Broken circuit while searching from FHIR server. Reason: {0}", bcEx.Message);

                throw new FhirSearchException(
                    string.Format(Resource.FhirSearchFailed, uri),
                    bcEx);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Unknown error while searching from FHIR server. Reason: {ex.Message}");
                _logger.LogError(ex, "Unhandled error while searching from FHIR server. Reason: {0}", ex.Message);
                throw;
            }
        }

        private string GetResponseFromHttpRequest(Uri uri)
        {
            try
            {
                HttpRequestMessage searchRequest = new HttpRequestMessage(HttpMethod.Get, uri);

                HttpResponseMessage response = _httpClient.Send(searchRequest);
                response.EnsureSuccessStatusCode();
                _logger.LogInformation("Successfully retrieved result for url: '{url}'.", uri);

                Stream stream = response.Content.ReadAsStream();
                stream.Seek(0, SeekOrigin.Begin);
                StreamReader reader = new StreamReader(stream);
                return reader.ReadToEnd();
            }
            catch (HttpRequestException ex)
            {
                switch (ex.StatusCode)
                {
                    case HttpStatusCode.Unauthorized:
                        _diagnosticLogger.LogError(string.Format("Failed to search from FHIR server: FHIR server {0} is unauthorized.", _dataSource.FhirServerUrl));
                        _logger.LogInformation(ex, "Failed to search from FHIR server: FHIR server {0} is unauthorized.", _dataSource.FhirServerUrl);
                        break;
                    case HttpStatusCode.NotFound:
                        _diagnosticLogger.LogError(string.Format("Failed to search from FHIR server: FHIR server {0} is not found.", _dataSource.FhirServerUrl));
                        _logger.LogInformation(ex, "Failed to search from FHIR server: FHIR server {0} is not found.", _dataSource.FhirServerUrl);
                        break;
                    default:
                        _diagnosticLogger.LogError(string.Format("Failed to search from FHIR server: Status code: {0}.", ex.StatusCode));
                        _logger.LogInformation(ex, "Failed to search from FHIR server: Status code: {0}.", ex.StatusCode);
                        break;
                }

                throw new FhirSearchException(
                    string.Format(Resource.FhirSearchFailed, uri),
                    ex);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Unknown error while searching from FHIR server. Reason: {ex.Message}");
                _logger.LogError(ex, "Unhandled error while searching from FHIR server. Reason: {0}", ex.Message);
                throw;
            }
        }
    }
}
