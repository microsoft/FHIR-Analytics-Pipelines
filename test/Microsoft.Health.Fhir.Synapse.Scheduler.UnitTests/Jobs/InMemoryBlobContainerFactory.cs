// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.Azure;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.UnitTests.Jobs
{
    public class InMemoryBlobContainerFactory : IAzureBlobContainerClientFactory
    {
        public IAzureBlobContainerClient Create(string storeUrl, string containerName)
        {
            return new InMemoryBlobContainerClient();
        }
    }
}
