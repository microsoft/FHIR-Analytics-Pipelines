// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Azure.Blob
{
    public interface IAzureBlobContainerClientFactory
    {
        public IAzureBlobContainerClient Create(string storeUrl, string containerName);
    }
}
