// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Logging;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Api
{
    /// <summary>
    /// Provide access token for the given url from token credential.
    /// Access token will be cached and will be refreshed if expired.
    /// </summary>
    public class AzureAccessTokenProvider : IAccessTokenProvider
    {
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<AzureAccessTokenProvider> _logger;
        private readonly TokenCredential _tokenCredential;
        private ConcurrentDictionary<string, AccessToken> _accessTokenDic = new ConcurrentDictionary<string, AccessToken>();
        private const int TokenExpireInterval = 5;

        public AzureAccessTokenProvider(TokenCredential tokenCredential, IDiagnosticLogger diagnosticLogger, ILogger<AzureAccessTokenProvider> logger)
        {
            EnsureArg.IsNotNull(tokenCredential, nameof(tokenCredential));
            EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _diagnosticLogger = diagnosticLogger;
            _logger = logger;
            _tokenCredential = tokenCredential;
        }

        public async Task<string> GetAccessTokenAsync(string resourceUrl, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNullOrWhiteSpace(resourceUrl, nameof(resourceUrl));

            try
            {
                if (!_accessTokenDic.TryGetValue(resourceUrl, out AccessToken accessToken) || string.IsNullOrEmpty(accessToken.Token) || accessToken.ExpiresOn < DateTime.UtcNow.AddMinutes(TokenExpireInterval))
                {
                    string[] scopes = { resourceUrl.TrimEnd('/') + "/.default" };
                    accessToken = await _tokenCredential.GetTokenAsync(new TokenRequestContext(scopes), cancellationToken);
                    _accessTokenDic.AddOrUpdate(resourceUrl, accessToken, (key, value) => accessToken);
                }

                _logger.LogInformation("Get access token for resource '{0}' successfully.", resourceUrl);
                return accessToken.Token;
            }
            catch (Exception exception)
            {
                _diagnosticLogger.LogError(string.Format("Get access token for resource '{0}' failed. Reason: '{1}'", resourceUrl, exception.Message));
                _logger.LogInformation(exception, "Get access token for resource '{0}' failed. Reason: '{1}'", resourceUrl, exception.Message);
                throw;
            }
        }
    }
}
