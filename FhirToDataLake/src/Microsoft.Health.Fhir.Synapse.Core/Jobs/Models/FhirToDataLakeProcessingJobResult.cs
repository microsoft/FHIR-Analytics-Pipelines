// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeProcessingJobResult
    {
        public DateTimeOffset ProcessingStartTime { get; set; }

        public DateTimeOffset ProcessingCompleteTime { get; set; }
        /// <summary>
        /// Search count for each resource type.
        /// </summary>
        [JsonProperty("searchCount")]
        public Dictionary<string, int> SearchCount { get; set; }

        /// <summary>
        /// Skipped count for each schema type.
        /// </summary>
        [JsonProperty("skippedCount")]
        public Dictionary<string, int> SkippedCount { get; set; }

        /// <summary>
        /// Processed count for each schema type.
        /// </summary>
        [JsonProperty("processedCount")]
        public Dictionary<string, int> ProcessedCount { get; set; }

        public Dictionary<string, int> ProcessedPatientVersion { get; set; }
    }
}
