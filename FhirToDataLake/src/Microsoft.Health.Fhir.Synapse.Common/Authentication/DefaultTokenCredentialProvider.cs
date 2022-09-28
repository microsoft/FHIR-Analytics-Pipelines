// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Core;
using Azure.Identity;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Logging;

namespace Microsoft.Health.Fhir.Synapse.Common.Authentication
{
    public class DefaultTokenCredentialProvider : ITokenCredentialProvider
    {
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<DefaultTokenCredentialProvider> _logger;

        public DefaultTokenCredentialProvider(IDiagnosticLogger diagnosticLogger, ILogger<DefaultTokenCredentialProvider> logger)
        {
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public TokenCredential GetCredential(TokenCredentialTypes type)
        {
            var credential = new DefaultAzureCredential();

            _diagnosticLogger.LogInformation($"Get {type} token credential successfully.");
            _logger.LogInformation($"Get {type} token credential successfully.");

            return credential;
        }
    }
}
