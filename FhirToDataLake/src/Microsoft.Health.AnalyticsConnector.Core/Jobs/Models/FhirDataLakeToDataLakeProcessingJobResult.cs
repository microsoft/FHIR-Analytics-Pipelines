// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs.Models
{
    public class FhirDataLakeToDataLakeProcessingJobResult
    {
        /// <summary>
        /// Processing job start time
        /// </summary>
        public DateTimeOffset ProcessingStartTime { get; set; } = DateTimeOffset.UtcNow;

        /// <summary>
        /// Processing job complete time
        /// </summary>
        public DateTimeOffset? ProcessingCompleteTime { get; set; }

        /// <summary>
        /// Search count for each resource type.
        /// </summary>
        public Dictionary<string, int> SearchCount { get; set; } = new Dictionary<string, int>();

        /// <summary>
        /// Skipped count for each schema type.
        /// </summary>
        public Dictionary<string, int> SkippedCount { get; set; } = new Dictionary<string, int>();

        /// <summary>
        /// Processed count for each schema type.
        /// </summary>
        public Dictionary<string, int> ProcessedCount { get; set; } = new Dictionary<string, int>();

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