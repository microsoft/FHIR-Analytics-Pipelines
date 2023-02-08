// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations
{
    public class HealthCheckConfiguration
    {
        /// <summary>
        /// The timeout to use for a single health check
        /// </summary>
        [JsonProperty("healthCheckTimeoutInSeconds")]
        public int HealthCheckTimeoutInSeconds { get; set; } = 100;

        /// <summary>
        /// Time interval for health check.
        /// </summary>
        [JsonProperty("healthCheckTimeIntervalInSeconds")]
        public int HealthCheckTimeIntervalInSeconds { get; set; } = 120;
    }
}
