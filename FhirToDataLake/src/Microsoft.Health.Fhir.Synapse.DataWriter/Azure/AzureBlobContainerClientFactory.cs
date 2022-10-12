// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.Azure
{
    public class AzureBlobContainerClientFactory : IAzureBlobContainerClientFactory
    {
        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";
        private readonly ILoggerFactory _loggerFactory;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ITokenCredentialProvider _credentialProvider;
        private readonly StorageConfiguration _storageConfiguration;

        public AzureBlobContainerClientFactory(
            ITokenCredentialProvider credentialProvider,
            IOptions<StorageConfiguration> storageConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILoggerFactory loggerFactory)
        {
            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
            EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));
            EnsureArg.IsNotNull(storageConfiguration, nameof(storageConfiguration));
            EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));

            _credentialProvider = credentialProvider;
            _loggerFactory = loggerFactory;
            _storageConfiguration = storageConfiguration.Value;
            _diagnosticLogger = diagnosticLogger;
        }

        public IAzureBlobContainerClient Create(string storeUrl, string containerName, TokenCredentialTypes type = TokenCredentialTypes.External)
        {
            EnsureArg.IsNotNull(storeUrl, nameof(storeUrl));
            EnsureArg.IsNotNull(containerName, nameof(containerName));

            // Create client for local emulator.
            if (string.Equals(storeUrl, StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return new AzureBlobContainerClient(StorageEmulatorConnectionString, containerName, _diagnosticLogger, _loggerFactory.CreateLogger<AzureBlobContainerClient>());
            }

            var connectionString = type == TokenCredentialTypes.Internal ?
                _storageConfiguration.InternalStorageConnectionString : _storageConfiguration.ExternalStorageConnectionString;
            if (!string.IsNullOrEmpty(connectionString))
            {
                return new AzureBlobContainerClient(connectionString, containerName, _diagnosticLogger, _loggerFactory.CreateLogger<AzureBlobContainerClient>());
            }

            var storageUri = new Uri(storeUrl);
            var containerUrl = new Uri(storageUri, containerName);

            return new AzureBlobContainerClient(
                containerUrl,
                _credentialProvider,
                _diagnosticLogger,
                _loggerFactory.CreateLogger<AzureBlobContainerClient>());
        }
    }
}
