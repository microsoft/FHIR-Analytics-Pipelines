// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.Azure
{
    public class AzureBlobContainerClientFactory : IAzureBlobContainerClientFactory
    {
        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";
        private readonly ILoggerFactory _loggerFactory;

        public AzureBlobContainerClientFactory(
            ILoggerFactory loggerFactory)
        {
            EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));

            _loggerFactory = loggerFactory;
        }

        public IAzureBlobContainerClient Create(string storeUrl, string containerName)
        {
            EnsureArg.IsNotNull(storeUrl, nameof(storeUrl));
            EnsureArg.IsNotNull(containerName, nameof(containerName));

            // Create client for local emulator.
            if (string.Equals(storeUrl, StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return new AzureBlobContainerClient(StorageEmulatorConnectionString, containerName, _loggerFactory.CreateLogger<AzureBlobContainerClient>());
            }

            var storageUri = new Uri(storeUrl);
            var containerUrl = new Uri(storageUri, containerName);

            return new AzureBlobContainerClient(
                containerUrl,
                _loggerFactory.CreateLogger<AzureBlobContainerClient>());
        }
    }
}
