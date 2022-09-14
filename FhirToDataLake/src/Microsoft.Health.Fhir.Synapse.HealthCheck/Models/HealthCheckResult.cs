// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Text.Json.Serialization;
using EnsureThat;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Models
{
    public class HealthCheckResult
    {
        public HealthCheckResult(string name, bool isCritical = false)
        {
            Name = EnsureArg.IsNotNull(name, nameof(name));
            StartTime = DateTimeOffset.UtcNow;
            IsCritical = isCritical;
        }

        public string Name { get; }

        [JsonConverter(typeof(JsonStringEnumConverter))]
        public HealthCheckStatus Status { get; set; } = HealthCheckStatus.HEALTHY;

        /// <summary>
        /// Indicates if the failure is critical.
        /// </summary>
        public bool IsCritical { get; }

        public string ErrorMessage { get; set; }

        /// <summary>
        /// The time the Health Check started.
        /// </summary>
        public DateTimeOffset StartTime { get; set; }
    }
}
