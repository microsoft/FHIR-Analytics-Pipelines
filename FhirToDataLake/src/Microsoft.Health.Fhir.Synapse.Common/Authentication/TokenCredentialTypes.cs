// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Authentication
{
    public enum TokenCredentialTypes
    {
        /// <summary>
        /// For external resources
        /// </summary>
        External,

        /// <summary>
        /// For internal resources
        /// </summary>
        Internal,
    }
}
