// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
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
        private Dictionary<string, AccessToken> _accessTokenDic = new ();

        public AzureAccessTokenProvider(ICredentialProvider credentialProvider, ILogger<AzureAccessTokenProvider> logger)
        {
            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _logger = logger;
            _tokenCredential = credentialProvider.GetCredential();
        }

        public async Task<string> GetAccessTokenAsync(string resourceUrl, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNullOrWhiteSpace(resourceUrl, nameof(resourceUrl));

            try
            {
                var accessToken = _accessTokenDic.GetValueOrDefault(resourceUrl);

                if (string.IsNullOrEmpty(accessToken.Token) || accessToken.ExpiresOn < DateTime.UtcNow.AddMinutes(1))
                {
                    var uri = new Uri(resourceUrl);
                    var scopes = new string[] { uri.ToString().EndsWith(@"/", StringComparison.InvariantCulture) ? uri + ".default" : uri + "/.default" };
                    var requestContext = new TokenRequestContext(scopes);
                    accessToken = await _tokenCredential.GetTokenAsync(requestContext, cancellationToken);
                    _accessTokenDic[resourceUrl] = accessToken;
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
