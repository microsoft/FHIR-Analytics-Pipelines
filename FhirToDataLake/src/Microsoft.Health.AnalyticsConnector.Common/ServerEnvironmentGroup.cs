// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.Common
{
    /// <summary>
    /// Server environment group
    /// </summary>
    public enum ServerEnvironmentGroup
    {
        /// <summary>
        /// Azure production cloud environment
        /// </summary>
        PROD,

        /// <summary>
        /// Local environment
        /// </summary>
        LOCAL,

        /// <summary>
        /// Test environment
        /// </summary>
        TEST,
    }
}
