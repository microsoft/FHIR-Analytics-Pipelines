// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using System;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests
{
    public class MockAzureBlobContainerClientFactory : IAzureBlobContainerClientFactory
    {
        private readonly IAzureBlobContainerClient _client;

        public MockAzureBlobContainerClientFactory(IAzureBlobContainerClient client)
        {
            _client = client;
        }

        public IAzureBlobContainerClient Create(string storeUrl, string containerName)
        {
            return _client;
        }
    }
}
