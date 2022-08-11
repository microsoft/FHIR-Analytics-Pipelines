// --------------------------------------------------------------------------
// <copyright file="HealthCheckResult.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>
// --------------------------------------------------------------------------

using System;
using System.Text.Json.Serialization;
using EnsureThat;
using Newtonsoft.Json.Converters;

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

        [JsonConverter(typeof(StringEnumConverter))]
        public HealthCheckStatus Status { get; set; } = HealthCheckStatus.UNKNOWN;

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
