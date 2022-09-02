// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.Common.Authentication;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.Azure
{
    public interface IAzureBlobContainerClientFactory
    {
        public IAzureBlobContainerClient Create(string storeUrl, string containerName, TokenCredentialTypes type = TokenCredentialTypes.External);
    }
}
