// --------------------------------------------------------------------------
// <copyright file="HealthCheckResult.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>
// --------------------------------------------------------------------------

using System;
using System.Text.Json.Serialization;
using EnsureThat;
using Newtonsoft.Json.Converters;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Models
{
    public class HealthCheckResult
    {
        public HealthCheckResult(string name)
        {
            Name = EnsureArg.IsNotNullOrEmpty(name, nameof(name));
            StartTime = DateTime.UtcNow;
        }

        public string Name { get; }

        [JsonConverter(typeof(StringEnumConverter))]
        public HealthCheckStatus Status { get; set; } = HealthCheckStatus.UNKNOWN;

        public string ErrorMessage { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        public HealthCheckStatus AzureBlobStorageWriteStatus { get; set; } = HealthCheckStatus.UNKNOWN;

        [JsonConverter(typeof(StringEnumConverter))]
        public HealthCheckStatus AzureBlobStorageReadStatus { get; set; } = HealthCheckStatus.UNKNOWN;

        [JsonConverter(typeof(StringEnumConverter))]
        public HealthCheckStatus AzureBlobStorageDeleteStatus { get; set; } = HealthCheckStatus.UNKNOWN;

        [JsonConverter(typeof(StringEnumConverter))]
        public HealthCheckStatus FhirServiceSearchStatus { get; set; } = HealthCheckStatus.UNKNOWN;

        /// <summary>
        /// The time the Health Check started
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// The time the Health Check finished
        /// </summary>
        public DateTime EndTime { get; set; }
    }
}
