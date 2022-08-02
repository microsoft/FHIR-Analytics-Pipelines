// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class HealthCheckConfiguration
    {
        /// <summary>
        /// The timeout to use for a single health check
        /// </summary>
        [JsonProperty("healthCheckTimeout")]
        public double HealthCheckTimeoutInSeconds { get; set; } = 25;

        /// <summary>
        /// Time interval for health check.
        /// </summary>
        [JsonProperty("healthCheckTimeInterval")]
        public double HealthCheckTimeIntervalInSeconds { get; set; } = 30;
    }
}
