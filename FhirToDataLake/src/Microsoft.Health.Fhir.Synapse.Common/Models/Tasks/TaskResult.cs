// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    public class TaskResult
    {
        public TaskResult(
            string resourceType,
            string continuationToken,
            Dictionary<string, int> partId,
            int searchCount,
            Dictionary<string, int> skippedCount,
            Dictionary<string, int> processedCount,
            string result)
        {
            ResourceType = resourceType;
            ContinuationToken = continuationToken;
            PartId = partId;
            SearchCount = searchCount;
            SkippedCount = skippedCount;
            ProcessedCount = processedCount;
            Result = result;
        }

        /// <summary>
        /// Resource type.
        /// </summary>
        public string ResourceType { get; set; }

        /// <summary>
        /// Task progress.
        /// </summary>
        public string ContinuationToken { get; set; }

        /// <summary>
        /// Part id for task output files.
        /// The format is '{Resource}_{JobId}_{PartId}.parquet', e.g. Patient_1ab3edcefsi789ed_0001.parquet.
        /// </summary>
        public Dictionary<string, int> PartId { get; set; }

        /// <summary>
        /// Search count.
        /// </summary>
        public int SearchCount { get; set; }

        /// <summary>
        /// Skipped count.
        /// </summary>
        public Dictionary<string, int> SkippedCount { get; set; }

        /// <summary>
        /// Processed count.
        /// </summary>
        public Dictionary<string, int> ProcessedCount { get; set; }

        /// <summary>
        /// Text result message.
        /// </summary>
        public string Result { get; set; }

        public static TaskResult CreateFromTaskContext(TaskContext context)
        {
            return new TaskResult(
                context.ResourceType,
                context.ContinuationToken,
                context.PartId,
                context.SearchCount,
                context.SkippedCount,
                context.ProcessedCount,
                JsonConvert.SerializeObject(context));
        }
    }
}
