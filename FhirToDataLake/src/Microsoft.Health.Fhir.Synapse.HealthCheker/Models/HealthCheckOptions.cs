// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Models
{
    public class HealthCheckOptions
    {
        public const string Settings = "HealthCheck";

        /// <summary>
        /// The location of the file to write out health check results
        /// </summary>
        public string OutputFile { get; set; } = "/tmp/healthCheck.json";

        /// <summary>
        /// The amount of time to wait between invoking health checks
        /// </summary>
        public TimeSpan CheckInterval { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// The timeout to use for a single health check
        /// </summary>
        public TimeSpan HealthCheckTimeout { get; set; } = TimeSpan.FromSeconds(25);
    }
}
