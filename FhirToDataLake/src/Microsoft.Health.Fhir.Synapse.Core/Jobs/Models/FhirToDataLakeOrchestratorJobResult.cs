// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeOrchestratorJobResult
    {
        public DateTimeOffset CreateTime { get; set; } = DateTimeOffset.UtcNow;

        public long CreatedJobCount { get; set; }

        public DateTimeOffset? NextJobTimestamp { get; set; }

        public int NextPatientIndex { get; set; }

        public ISet<long> RunningJobIds { get; set; } = new HashSet<long>();

        /// <summary>
        /// Total resource count (from data source) for each resource types.
        /// </summary>
        [JsonProperty("totalResourceCounts")]
        public Dictionary<string, int> TotalResourceCounts { get; set; } = new Dictionary<string, int>();

        /// <summary>
        /// Processed resource count for each schema type.
        /// </summary>
        [JsonProperty("processedResourceCounts")]
        public Dictionary<string, int> ProcessedResourceCounts { get; set; } = new Dictionary<string, int>();

        /// <summary>
        /// Skipped resource count for each schema type.
        /// </summary>
        [JsonProperty("skippedResourceCounts")]
        public Dictionary<string, int> SkippedResourceCounts { get; set; } = new Dictionary<string, int>();
    }
}