// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Models
{
    public class HealthCheckOptions
    {
        /// <summary>
        /// The timeout to use for health results collection
        /// </summary>
        public TimeSpan HealthStatusCollectionTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// The timeout to use for a single health check
        /// </summary>
        public TimeSpan HealthCheckTimeout { get; set; } = TimeSpan.FromSeconds(25);
    }
}
