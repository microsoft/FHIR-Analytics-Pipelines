// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;

namespace Microsoft.Health.AnalyticsConnector.HealthCheck.UnitTests
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
