// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public interface IAzureStorageClientFactory
    {
        public IAzureStorageClient Create(IStorage storage);
    }
}