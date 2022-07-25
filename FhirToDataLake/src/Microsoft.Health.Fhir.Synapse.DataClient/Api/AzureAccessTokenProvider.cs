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

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    /// <summary>
    /// Provide access token for the given FHIR url from token credential.
    /// Access token will be cached and will be refreshed if expired.
    /// </summary>
    public class AzureAccessTokenProvider : IAccessTokenProvider
    {
        private readonly ILogger<AzureAccessTokenProvider> _logger;
        private readonly TokenCredential _tokenCredential;
        private ConcurrentDictionary<string, AccessToken> _accessTokenDic = new ();
        private const int _tokenExpireInterval = 5;

        public AzureAccessTokenProvider(TokenCredential tokenCredential, ILogger<AzureAccessTokenProvider> logger)
        {
            EnsureArg.IsNotNull(tokenCredential, nameof(tokenCredential));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _logger = logger;
            _tokenCredential = tokenCredential;
        }

        public async Task<string> GetAccessTokenAsync(string resourceUrl, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNullOrWhiteSpace(resourceUrl, nameof(resourceUrl));

            try
            {
                if (!_accessTokenDic.TryGetValue(resourceUrl, out AccessToken accessToken) || string.IsNullOrEmpty(accessToken.Token) || accessToken.ExpiresOn < DateTime.UtcNow.AddMinutes(_tokenExpireInterval))
                {
                    var scopes = new string[] { resourceUrl.TrimEnd('/') + "/.default" };
                    accessToken = await _tokenCredential.GetTokenAsync(new TokenRequestContext(scopes), cancellationToken);
                    _accessTokenDic.AddOrUpdate(resourceUrl, accessToken, (key, value) => accessToken);
                }

                _logger.LogInformation("Get access token for resource '{0}' successfully.", resourceUrl);
                return accessToken.Token;
            }
            catch (Exception exception)
            {
                _logger.LogError("Get access token for resource '{0}' failed. Reason: '{1}'", resourceUrl, exception);
                throw;
            }
        }
    }
}
