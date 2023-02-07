// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.Common.Metrics
{
    public enum MetricsCategories
    {
        /// <summary>
        /// An metric category for errors
        /// </summary>
        Errors,

        /// <summary>
        /// A metric category for traffic and requests
        /// </summary>
        Traffic,

        /// <summary>
        /// A metric category for service availability
        /// </summary>
        Availability,

        /// <summary>
        /// A metric category for request latency
        /// </summary>
        Latency,

        /// <summary>
        /// A metric category for component health
        /// </summary>
        Health,
    }
}