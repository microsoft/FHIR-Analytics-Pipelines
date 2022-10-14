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

namespace Microsoft.Health.Fhir.Synapse.DataWriter.Azure
{
    public class AzureBlobContainerClientFactory : IAzureBlobContainerClientFactory
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ITokenCredentialProvider _credentialProvider;
        private readonly string _externalConnectionString;

        public AzureBlobContainerClientFactory(
            ITokenCredentialProvider credentialProvider,
            IOptions<StorageConfiguration> storageConfiguration,
            ILoggerFactory loggerFactory)
        {
            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
            EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));
            EnsureArg.IsNotNull(storageConfiguration, nameof(storageConfiguration));

            _credentialProvider = credentialProvider;
            _loggerFactory = loggerFactory;
            _externalConnectionString = storageConfiguration.Value.ExternalStorageConnectionString;
        }

        public IAzureBlobContainerClient Create(string storeUrl, string containerName)
        {
            EnsureArg.IsNotNull(storeUrl, nameof(storeUrl));
            EnsureArg.IsNotNull(containerName, nameof(containerName));

            // Create client for local emulator.
            if (string.Equals(storeUrl, ConfigurationConstants.StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return new AzureBlobContainerClient(ConfigurationConstants.StorageEmulatorConnectionString, containerName, _loggerFactory.CreateLogger<AzureBlobContainerClient>());
            }

            if (!string.IsNullOrEmpty(_externalConnectionString))
            {
                return new AzureBlobContainerClient(_externalConnectionString, containerName, _loggerFactory.CreateLogger<AzureBlobContainerClient>());
            }

            var storageUri = new Uri(storeUrl);
            var containerUrl = new Uri(storageUri, containerName);

            return new AzureBlobContainerClient(
                containerUrl,
                _credentialProvider,
                _loggerFactory.CreateLogger<AzureBlobContainerClient>());
        }
    }
}
