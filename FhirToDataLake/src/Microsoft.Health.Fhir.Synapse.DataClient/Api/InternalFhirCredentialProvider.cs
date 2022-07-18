// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure.Core;
using Azure.Identity;
using EnsureThat;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public class InternalFhirCredentialProvider : IFhirCredentialProvider
    {

        private readonly ILogger<InternalFhirCredentialProvider> _logger;

        public InternalFhirCredentialProvider(ILogger<InternalFhirCredentialProvider> logger)
        {
            EnsureArg.IsNotNull(logger, nameof(logger));

            _logger = logger;
        }

        public TokenCredential GetCredential()
        {
            try
            {
                var credential = new DefaultAzureCredential();
                _logger.LogInformation("Get internal token credential successfully.");
                return credential;
            }
            catch (Exception exception)
            {
                _logger.LogError("Get internal token credential failed. Reason: '{0}'", exception);
                throw;
            }
        }
    }
}
