// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Synapse.Azure
{
    public class LocalAzureBlobContainerClientFactory : IAzureBlobContainerClientFactory
    {
        public const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";

        private readonly ILoggerFactory _loggerFactory;

        public LocalAzureBlobContainerClientFactory(
            ILoggerFactory loggerFactory)
        {
            EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));

            _loggerFactory = loggerFactory;
        }

        public IAzureBlobContainerClient Create(string storeUrl, string containerName)
        {
            EnsureArg.IsNotNull(containerName, nameof(containerName));

            return new AzureBlobContainerClient(
                StorageEmulatorConnectionString,
                containerName,
                _loggerFactory.CreateLogger<AzureBlobContainerClient>());
        }
    }
}
