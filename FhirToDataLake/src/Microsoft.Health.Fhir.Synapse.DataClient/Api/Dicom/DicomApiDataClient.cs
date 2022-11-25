// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
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
using Microsoft.Health.Fhir.Synapse.DataClient.Models;
using Polly.CircuitBreaker;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api.Dicom
{
    public class DicomApiDataClient : IApiDataClient
    {
        private readonly IApiDataSource _dicomApiDataSource;
        private readonly HttpClient _httpClient;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly IAccessTokenProvider _accessTokenProvider;
        private readonly ILogger<DicomApiDataClient> _logger;

        public DicomApiDataClient(
            IApiDataSource dataSource,
            HttpClient httpClient,
            ITokenCredentialProvider tokenCredentialProvider,
            IDiagnosticLogger diagnosticLogger,
            ILogger<DicomApiDataClient> logger)
        {
            EnsureArg.IsNotNull(dataSource, nameof(dataSource));
            EnsureArg.IsNotNullOrEmpty(dataSource.ServerUrl, nameof(dataSource.ServerUrl));
            EnsureArg.IsNotNull(httpClient, nameof(httpClient));
            EnsureArg.IsNotNull(tokenCredentialProvider, nameof(tokenCredentialProvider));
            EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dicomApiDataSource = dataSource;
            _httpClient = httpClient;
            _httpClient.DefaultRequestHeaders.Add("Accept", "application/dicom+json");
            _accessTokenProvider = new AzureAccessTokenProvider(
                tokenCredentialProvider.GetCredential(TokenCredentialTypes.External),
                diagnosticLogger,
                new Logger<AzureAccessTokenProvider>(new LoggerFactory()));
            _diagnosticLogger = diagnosticLogger;
            _logger = logger;
        }

        public async Task<string> SearchAsync(
            BaseApiOptions dicomApiOptions,
            CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }

            Uri searchUri;
            try
            {
                searchUri = CreateSearchUri(dicomApiOptions);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError(string.Format("Create search Uri failed, Reason: '{0}'", ex.Message));
                _logger.LogInformation(ex, "Create search Uri failed, Reason: '{reason}'", ex.Message);
                throw new ApiSearchException("Create search Uri failed", ex);
            }

            string accessToken = null;
            if (dicomApiOptions.IsAccessTokenRequired)
            {
                try
                {
                    if (_dicomApiDataSource.Authentication == AuthenticationType.ManagedIdentity)
                    {
                        // Currently we support accessing service endpoints with Managed Identity.

                        // The thread-safe AzureServiceTokenProvider class caches the token in memory and retrieves it from Azure AD just before expiration.
                        // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/service-to-service-authentication#using-the-library
                        accessToken = await _accessTokenProvider.GetAccessTokenAsync(DicomApiConstants.DicomResourceUrl, cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    _diagnosticLogger.LogError(string.Format("Get server access token failed, Reason: '{0}'", ex.Message));
                    _logger.LogInformation(ex, "Get server access token failed, Reason: '{reason}'", ex.Message);
                    throw new ApiSearchException("Get server access token failed", ex);
                }
            }

            return await GetResponseFromHttpRequestAsync(searchUri, accessToken, cancellationToken);
        }

        public string Search(BaseApiOptions serverApiOptions)
        {
            _diagnosticLogger.LogError("Synchronous search is not supported in DICOM.");
            _logger.LogInformation("Synchronous search is not supported in DICOM.");
            throw new ApiSearchException("Synchronous search is not supported in DICOM.");
        }

        private Uri CreateSearchUri(BaseApiOptions dicomApiOptions)
        {
            string serverUrl = _dicomApiDataSource.ServerUrl;

            var baseUri = new Uri(serverUrl);

            var uri = new Uri(baseUri, dicomApiOptions.RelativeUri());

            if (dicomApiOptions.QueryParameters == null)
            {
                return uri;
            }

            return uri.AddQueryString(dicomApiOptions.QueryParameters);
        }

        private async Task<string> GetResponseFromHttpRequestAsync(Uri uri, string accessToken = null, CancellationToken cancellationToken = default)
        {
            try
            {
                var searchRequest = new HttpRequestMessage(HttpMethod.Get, uri);
                if (accessToken != null)
                {
                    // Currently we support accessing service endpoints with Managed Identity.

                    // The thread-safe AzureServiceTokenProvider class caches the token in memory and retrieves it from Azure AD just before expiration.
                    // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/service-to-service-authentication#using-the-library
                    searchRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
                }

                HttpResponseMessage response = await _httpClient.SendAsync(searchRequest, cancellationToken);
                response.EnsureSuccessStatusCode();

                _logger.LogInformation("Successfully retrieved result for url: '{url}'.", uri);

                return await response.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (HttpRequestException hrEx)
            {
                switch (hrEx.StatusCode)
                {
                    case HttpStatusCode.Unauthorized:
                        _diagnosticLogger.LogError(string.Format("Failed to search from server: Server {0} is unauthorized.", _dicomApiDataSource.ServerUrl));
                        _logger.LogInformation(hrEx, "Failed to search from server: Server {0} is unauthorized.", _dicomApiDataSource.ServerUrl);
                        break;
                    case HttpStatusCode.NotFound:
                        _diagnosticLogger.LogError(string.Format("Failed to search from server: Server {0} is not found.", _dicomApiDataSource.ServerUrl));
                        _logger.LogInformation(hrEx, "Failed to search from server: Server {0} is not found.", _dicomApiDataSource.ServerUrl);
                        break;
                    case HttpStatusCode.Forbidden:
                        _diagnosticLogger.LogError(string.Format("Failed to search from server: Server {0} is forbidden.", _dicomApiDataSource.ServerUrl));
                        _logger.LogInformation(hrEx, "Failed to search from server: Server {0} is forbidden.", _dicomApiDataSource.ServerUrl);
                        break;
                    default:
                        _diagnosticLogger.LogError(string.Format("Failed to search from server: Status code: {0}. Reason: {1}", hrEx.StatusCode, hrEx.Message));
                        _logger.LogInformation(hrEx, "Failed to search from server: Status code: {0}. Reason: {1}", hrEx.StatusCode, hrEx.Message);
                        break;
                }

                throw new ApiSearchException(
                    string.Format(Resource.FhirSearchFailed, uri, hrEx.Message),
                    hrEx);
            }
            catch (BrokenCircuitException bcEx)
            {
                _diagnosticLogger.LogError($"Failed to search from server. Reason: {bcEx.Message}");
                _logger.LogInformation(bcEx, "Broken circuit while searching from server. Reason: {0}", bcEx.Message);

                throw new ApiSearchException(
                    string.Format(Resource.FhirSearchFailed, uri, bcEx.Message),
                    bcEx);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Unknown error while searching from server. Reason: {ex.Message}");
                _logger.LogError(ex, "Unhandled error while searching from server. Reason: {0}", ex.Message);
                throw;
            }
        }
    }
}
