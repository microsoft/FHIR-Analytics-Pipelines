// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public class AzureAccessTokenProvider : IAccessTokenProvider
    {
        private readonly AzureServiceTokenProvider _azureTokenProvider;
        private readonly ILogger<AzureAccessTokenProvider> _logger;

        public AzureAccessTokenProvider(ILogger<AzureAccessTokenProvider> logger)
        {
            EnsureArg.IsNotNull(logger, nameof(logger));

            _azureTokenProvider = new AzureServiceTokenProvider();
            _logger = logger;
        }

        public async Task<string> GetAccessTokenAsync(
            string resourceUrl,
            CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNullOrWhiteSpace(resourceUrl, nameof(resourceUrl));

            string token;
            try
            {
                token = await _azureTokenProvider.GetAccessTokenAsync(resourceUrl, cancellationToken: cancellationToken);
            }
            catch (Exception exception)
            {
                _logger.LogError("Get access token for resource '{0}' failed. Reason: '{1}'", resourceUrl, exception);
                throw;
            }

            _logger.LogInformation("Get access token for resource '{0}' successfully.", resourceUrl);
            return token;
        }
    }
}
