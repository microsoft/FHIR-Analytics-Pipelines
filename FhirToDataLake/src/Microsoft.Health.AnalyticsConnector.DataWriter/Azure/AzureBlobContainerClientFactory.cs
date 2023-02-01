// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Authentication;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;

namespace Microsoft.Health.AnalyticsConnector.DataWriter.Azure
{
    public class AzureBlobContainerClientFactory : IAzureBlobContainerClientFactory
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ITokenCredentialProvider _credentialProvider;
        private readonly string _externalConnectionString;

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
            _diagnosticLogger = diagnosticLogger;
            _externalConnectionString = storageConfiguration.Value.ExternalStorageConnectionString;
        }

        public IAzureBlobContainerClient Create(string storeUrl, string containerName)
        {
            EnsureArg.IsNotNull(storeUrl, nameof(storeUrl));
            EnsureArg.IsNotNull(containerName, nameof(containerName));

            // Create client for local emulator.
            if (string.Equals(storeUrl, ConfigurationConstants.StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return new AzureBlobContainerClient(ConfigurationConstants.StorageEmulatorConnectionString, containerName, _diagnosticLogger, _loggerFactory.CreateLogger<AzureBlobContainerClient>());
            }

            if (!string.IsNullOrEmpty(_externalConnectionString))
            {
                return new AzureBlobContainerClient(_externalConnectionString, containerName, _diagnosticLogger, _loggerFactory.CreateLogger<AzureBlobContainerClient>());
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
