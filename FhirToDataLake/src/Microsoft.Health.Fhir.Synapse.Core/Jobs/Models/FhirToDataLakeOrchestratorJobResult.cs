// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeOrchestratorJobResult
    {
        /// <summary>
        /// Job start time
        /// </summary>
        public DateTimeOffset StartTime { get; set; } = DateTimeOffset.UtcNow;

        /// <summary>
        /// Job complete time
        /// </summary>
        public DateTimeOffset? CompleteTime { get; set; }

        /// <summary>
        /// Created job count
        /// </summary>
        public long CreatedJobCount { get; set; }

        /// <summary>
        /// Next job timestamp to be processed, used for system scope
        /// </summary>
        public DateTimeOffset? NextJobTimestamp { get; set; }

        /// <summary>
        /// Processing resource type to be processed, used for system scope
        /// </summary>
        public Dictionary<string, DateTimeOffset> ProcessingResourceType { get; set; } = new Dictionary<string, DateTimeOffset>();

        /// <summary>
        /// Next patient index to be processed, used for group scope
        /// </summary>
        public int NextPatientIndex { get; set; }

        /// <summary>
        /// Running job ids
        /// </summary>
        public ISet<long> RunningJobIds { get; set; } = new HashSet<long>();

        /// <summary>
        /// Total resource count (from data source) for each resource types.
        /// </summary>
        public Dictionary<string, int> TotalResourceCounts { get; set; } = new Dictionary<string, int>();

        /// <summary>
        /// Processed resource count for each schema type.
        /// </summary>
        public Dictionary<string, int> ProcessedResourceCounts { get; set; } = new Dictionary<string, int>();

        /// <summary>
        /// Skipped resource count for each schema type.
        /// </summary>
        public Dictionary<string, int> SkippedResourceCounts { get; set; } = new Dictionary<string, int>();

        /// <summary>
        /// Output resource count in total.
        /// </summary>
        public int ProcessedCountInTotal { get; set; } = 0;

        /// <summary>
        /// Data size for output parquet data in bytes.
        /// </summary>
        public long ProcessedDataSizeInTotal { get; set; } = 0;
    }
}