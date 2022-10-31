// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Newtonsoft.Json;
using Polly;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry
{
    /// <summary>
    /// Retrieve ACR access token with AAD token provider.
    /// We need to exchange ACR refresh token with AAD token, and get ACR access token from refresh token.
    /// References:
    /// https://github.com/Azure/acr/blob/main/docs/AAD-OAuth.md#calling-post-oauth2exchange-to-get-an-acr-refresh-token
    /// https://github.com/Azure/acr/blob/main/docs/AAD-OAuth.md#calling-post-oauth2token-to-get-an-acr-access-token
    /// </summary>
    public class ContainerRegistryAccessTokenProvider : IContainerRegistryTokenProvider
    {
        private const string ExchangeAcrRefreshTokenUrl = "oauth2/exchange";
        private const string GetAcrAccessTokenUrl = "oauth2/token";

        private readonly IAccessTokenProvider _aadTokenProvider;
        private readonly HttpClient _client;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<ContainerRegistryAccessTokenProvider> _logger;

        public ContainerRegistryAccessTokenProvider(
            ITokenCredentialProvider tokenCredentialProvider,
            HttpClient httpClient,
            IDiagnosticLogger diagnosticLogger,
            ILogger<ContainerRegistryAccessTokenProvider> logger)
        {
            EnsureArg.IsNotNull(tokenCredentialProvider, nameof(tokenCredentialProvider));
            EnsureArg.IsNotNull(httpClient, nameof(httpClient));
            EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _aadTokenProvider = new AzureAccessTokenProvider(
                tokenCredentialProvider.GetCredential(TokenCredentialTypes.External),
                diagnosticLogger,
                new Logger<AzureAccessTokenProvider>(new LoggerFactory()));
            _client = httpClient;
            _diagnosticLogger = diagnosticLogger;
            _logger = logger;
        }

        public async Task<string> GetTokenAsync(string registryServer, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNullOrEmpty(registryServer, nameof(registryServer));

            string aadToken;
            try
            {
                aadToken = await _aadTokenProvider.GetAccessTokenAsync(ContainerRegistryConstants.ArmResourceManagerIdForAzureCloud, cancellationToken);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError("Failed to get AAD access token from managed identity.");
                _logger.LogInformation(ex, "Failed to get AAD access token from managed identity.");
                throw new ContainerRegistryTokenException("Failed to get AAD access token from managed identity.", ex);
            }

            try
            {
                return await Policy
                  .Handle<HttpRequestException>()
                  .WaitAndRetryAsync(
                    3,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        _logger.LogInformation(exception, "Get ACR token from {Server} failed. Retry {RetryCount}.", registryServer, retryCount);
                    })
                  .ExecuteAsync(() => GetAcrAccessTokenWithAadToken(registryServer, aadToken, cancellationToken));
            }
            catch (HttpRequestException ex)
            {
                _diagnosticLogger.LogError("Failed to get ACR access token with AAD access token.");
                _logger.LogInformation(ex, "Failed to get ACR access token with AAD access token.");
                throw new ContainerRegistryTokenException("Failed to get ACR access token with AAD access token.", ex);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError("Unknown error while getting ACR access token with AAD access token.");
                _logger.LogError(ex, "Unhandled error while getting ACR access token with AAD access token.");
                throw;
            }
        }

        private async Task<string> GetAcrAccessTokenWithAadToken(string registryServer, string aadToken, CancellationToken cancellationToken)
        {
            string acrRefreshToken = await ExchangeAcrRefreshToken(registryServer, aadToken, cancellationToken);
            return await GetAcrAccessToken(registryServer, acrRefreshToken, cancellationToken);
        }

        private async Task<string> ExchangeAcrRefreshToken(string registryServer, string aadToken, CancellationToken cancellationToken)
        {
            var registryUri = new Uri($"https://{registryServer}");
            var exchangeUri = new Uri(registryUri, ExchangeAcrRefreshTokenUrl);

            List<KeyValuePair<string, string>> parameters = new List<KeyValuePair<string, string>>();
            parameters.Add(new KeyValuePair<string, string>("grant_type", "access_token"));
            parameters.Add(new KeyValuePair<string, string>("service", registryUri.Host));
            parameters.Add(new KeyValuePair<string, string>("access_token", aadToken));
            using var request = new HttpRequestMessage(HttpMethod.Post, exchangeUri)
            {
                Content = new FormUrlEncodedContent(parameters),
            };

            HttpResponseMessage refreshTokenResponse = await SendRequestAsync(request, cancellationToken);
            if (!refreshTokenResponse.IsSuccessStatusCode)
            {
                switch (refreshTokenResponse.StatusCode)
                {
                    case HttpStatusCode.Unauthorized:
                        _diagnosticLogger.LogError(string.Format("Failed to exchange ACR refresh token: ACR server {0} is unauthorized.", registryServer));
                        _logger.LogInformation("Failed to exchange ACR refresh token: ACR server {0} is unauthorized.", registryServer);
                        throw new ContainerRegistryTokenException(string.Format("Failed to exchange ACR refresh token: ACR server {0} is unauthorized.", registryServer));
                    case HttpStatusCode.NotFound:
                        _diagnosticLogger.LogError(string.Format("Failed to exchange ACR refresh token: ACR server {0} is not found.", registryServer));
                        _logger.LogInformation("Failed to exchange ACR refresh token: ACR server {0} is not found.", registryServer);
                        throw new ContainerRegistryTokenException(string.Format("Failed to exchange ACR refresh token: ACR server {0} is not found.", registryServer));
                    default:
                        _diagnosticLogger.LogError(string.Format("Failed to exchange ACR refresh token with AAD access token. Status code: {0}.", refreshTokenResponse.StatusCode));
                        _logger.LogInformation("Failed to exchange ACR refresh token with AAD access token. Status code: {0}.", refreshTokenResponse.StatusCode);
                        throw new ContainerRegistryTokenException(string.Format("Failed to exchange ACR refresh token with AAD access token. Status code: {StatusCode}.", refreshTokenResponse.StatusCode));
                }
            }

            string refreshTokenText = await refreshTokenResponse.Content.ReadAsStringAsync(cancellationToken);
            dynamic refreshTokenJson = JsonConvert.DeserializeObject(refreshTokenText);
            string refreshToken = (string)refreshTokenJson.refresh_token;
            if (string.IsNullOrEmpty(refreshToken))
            {
                _diagnosticLogger.LogError("ACR refresh token is empty.");
                _logger.LogInformation("ACR refresh token is empty.");
                throw new ContainerRegistryTokenException("ACR refresh token is empty.");
            }

            _logger.LogInformation("Successfully exchanged ACR refresh token.");
            return refreshToken;
        }

        private async Task<string> GetAcrAccessToken(string registryServer, string refreshToken, CancellationToken cancellationToken)
        {
            var registryUri = new Uri($"https://{registryServer}");
            var accessTokenUri = new Uri(registryUri, GetAcrAccessTokenUrl);

            List<KeyValuePair<string, string>> parameters = new List<KeyValuePair<string, string>>();
            parameters.Add(new KeyValuePair<string, string>("grant_type", "refresh_token"));
            parameters.Add(new KeyValuePair<string, string>("service", registryUri.Host));
            parameters.Add(new KeyValuePair<string, string>("refresh_token", refreshToken));

            // Add scope for AcrPull role (granted at registry level).
            parameters.Add(new KeyValuePair<string, string>("scope", "repository:*:pull"));
            using var request = new HttpRequestMessage(HttpMethod.Post, accessTokenUri)
            {
                Content = new FormUrlEncodedContent(parameters),
            };

            HttpResponseMessage accessTokenResponse = await SendRequestAsync(request, cancellationToken);
            if (!accessTokenResponse.IsSuccessStatusCode)
            {
                switch (accessTokenResponse.StatusCode)
                {
                    case HttpStatusCode.Unauthorized:
                        _diagnosticLogger.LogError(string.Format("Failed to get ACR access token: ACR server {0} is unauthorized.", registryServer));
                        _logger.LogInformation("Failed to get ACR access token: ACR server {0} is unauthorized.", registryServer);
                        throw new ContainerRegistryTokenException(string.Format("Failed to get ACR access token: ACR server {0} is unauthorized.", registryServer));
                    case HttpStatusCode.NotFound:
                        _diagnosticLogger.LogError(string.Format("Failed to get ACR access token: ACR server {0} is not found.", registryServer));
                        _logger.LogInformation("Failed to get ACR access token: ACR server {0} is not found.", registryServer);
                        throw new ContainerRegistryTokenException(string.Format("Failed to get ACR access token: ACR server {0} is not found.", registryServer));
                    default:
                        _diagnosticLogger.LogError(string.Format("Failed to get ACR access token with ACR refresh token. Status code: {0}.", accessTokenResponse.StatusCode));
                        _logger.LogInformation("Failed to get ACR access token with ACR refresh token. Status code: {0}.", accessTokenResponse.StatusCode);
                        throw new ContainerRegistryTokenException(string.Format("Failed to get ACR access token with ACR refresh token. Status code: {StatusCode}.", accessTokenResponse.StatusCode));
                }
            }

            string accessTokenText = await accessTokenResponse.Content.ReadAsStringAsync(cancellationToken);
            dynamic accessTokenJson = JsonConvert.DeserializeObject(accessTokenText);
            string accessToken = accessTokenJson.access_token;
            if (string.IsNullOrEmpty(accessToken))
            {
                _diagnosticLogger.LogError("ACR access token is empty.");
                _logger.LogInformation("ACR access token is empty.");
                throw new ContainerRegistryTokenException("ACR access token is empty.");
            }

            _logger.LogInformation("Successfully retrieved ACR access token.");
            return $"Bearer {accessToken}";
        }

        private async Task<HttpResponseMessage> SendRequestAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return await _client.SendAsync(request, cancellationToken);
        }
    }
}
