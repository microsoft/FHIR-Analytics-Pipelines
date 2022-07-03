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
            bool isCompleted,
            Dictionary<string, int> searchCount,
            Dictionary<string, int> skippedCount,
            Dictionary<string, int> processedCount,
            string result)
        {
            IsCompleted = isCompleted;
            SearchCount = searchCount;
            SkippedCount = skippedCount;
            ProcessedCount = processedCount;
            Result = result;
        }

        /// <summary>
        /// Is this task completed.
        /// </summary>
        public bool IsCompleted { get; set; }

        /// <summary>
        /// Search count.
        /// </summary>
        public Dictionary<string, int> SearchCount { get; set; }

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
                context.IsCompleted,
                context.SearchCount,
                context.SkippedCount,
                context.ProcessedCount,
                JsonConvert.SerializeObject(context));
        }
    }
}
