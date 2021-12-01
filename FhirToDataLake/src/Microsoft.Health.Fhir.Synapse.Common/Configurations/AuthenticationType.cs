// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public enum AuthenticationType
    {
        // Access resource with managed identity
        ManagedIdentity,

        // Access resource with no authentication
        None,
    }
}
