// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations
{
    public enum AuthenticationType
    {
        /// <summary>
        /// Access resource with managed identity
        /// </summary>
        ManagedIdentity,

        /// <summary>
        /// Access resource with no authentication
        /// </summary>
        None,
    }
}
