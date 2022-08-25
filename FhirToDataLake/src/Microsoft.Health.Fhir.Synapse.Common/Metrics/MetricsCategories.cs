// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public static class MetricsCategories
    {
        /// <summary>
        /// An metric category for errors
        /// </summary>
        public static string Errors => nameof(MetricsCategories.Errors);

        /// <summary>
        /// A metric category for traffic and requests
        /// </summary>
        public static string Traffic => nameof(MetricsCategories.Traffic);

        /// <summary>
        /// A metric category for service availability
        /// </summary>
        public static string Availability => nameof(MetricsCategories.Availability);

        /// <summary>
        /// A metric category for request latency
        /// </summary>
        public static string Latency => nameof(MetricsCategories.Latency);
    }
}
