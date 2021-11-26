// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Synapse.Azure
{
    public class AzureBlobContainerClientFactory : IAzureBlobContainerClientFactory
    {
        private readonly ILoggerFactory _loggerFactory;

        public AzureBlobContainerClientFactory(
            ILoggerFactory loggerFactory)
        {
            EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));

            _loggerFactory = loggerFactory;
        }

        public IAzureBlobContainerClient Create(string storeUrl, string containerName)
        {
            EnsureArg.IsNotNull(containerName, nameof(containerName));

            var storageUri = new Uri(storeUrl);
            var containerUrl = new Uri(storageUri, containerName);

            return new AzureBlobContainerClient(
                containerUrl,
                _loggerFactory.CreateLogger<AzureBlobContainerClient>());
        }
    }
}
