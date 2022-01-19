// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    public class TaskResult
    {
        public TaskResult(
            string resourceType,
            string continuationToken,
            int partId,
            int searchCount,
            int skippedCount,
            int processedCount,
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
        public int PartId { get; set; }

        /// <summary>
        /// Search count.
        /// </summary>
        public int SearchCount { get; set; }

        /// <summary>
        /// Skipped count.
        /// </summary>
        public int SkippedCount { get; set; }

        /// <summary>
        /// Processed count.
        /// </summary>
        public int ProcessedCount { get; set; }

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
