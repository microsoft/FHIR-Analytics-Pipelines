// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure.Core;
using Azure.Identity;
using EnsureThat;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Synapse.Common.Authentication
{
    public class ExternalTokenCredentialProvider : ITokenCredentialProvider
    {

        private readonly ILogger<ExternalTokenCredentialProvider> _logger;

        public ExternalTokenCredentialProvider(ILogger<ExternalTokenCredentialProvider> logger)
        {
            EnsureArg.IsNotNull(logger, nameof(logger));

            _logger = logger;
        }

        public TokenCredential GetCredential()
        {
            try
            {
                var credential = new DefaultAzureCredential();
                _logger.LogInformation("Get external token credential successfully.");
                return credential;
            }
            catch (Exception exception)
            {
                _logger.LogError("Get external token credential failed. Reason: '{0}'", exception);
                throw;
            }
        }
    }
}
