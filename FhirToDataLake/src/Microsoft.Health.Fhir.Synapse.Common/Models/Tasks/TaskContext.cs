// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    public class TaskContext
    {
        // TODO: Refine this together with TaskExecutor class. Maybe this is more like a task class.
        public TaskContext(
            string id,
            string jobId,
            string resourceType,
            List<string> schemaTypes,
            DateTimeOffset startTime,
            DateTimeOffset endTime,
            string continuationToken,
            Dictionary<string, int> processedCount,
            Dictionary<string, int> skippedCount,
            Dictionary<string, int> partId,
            int searchCount = 0,
            bool isCompleted = false)
        {
            Id = id;
            JobId = jobId;
            ResourceType = resourceType;
            SchemaTypes = schemaTypes;
            StartTime = startTime;
            EndTime = endTime;
            ContinuationToken = continuationToken;
            SearchCount = searchCount;
            ProcessedCount = processedCount;
            SkippedCount = skippedCount;
            PartId = partId;
            IsCompleted = isCompleted;
        }

        /// <summary>
        /// Task id.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Job id.
        /// </summary>
        public string JobId { get; set; }

        /// <summary>
        /// Resource type for task.
        /// </summary>
        public string ResourceType { get; set; }

        /// <summary>
        /// Schema types for task.
        /// </summary>
        public List<string> SchemaTypes { get; set; }

        /// <summary>
        /// Start time for task.
        /// </summary>
        public DateTimeOffset StartTime { get; set; }

        /// <summary>
        /// End time for task.
        /// </summary>
        public DateTimeOffset EndTime { get; set; }

        /// <summary>
        /// Part id for for each schema type, the value will be appended to output files.
        /// The format is '{SchemaType}_{JobId}_{PartId}.parquet', e.g. Patient_1ab3edcefsi789ed_0001.parquet.
        /// </summary>
        public Dictionary<string, int> PartId { get; set; }

        /// <summary>
        /// Task progress.
        /// </summary>
        public string ContinuationToken { get; set; }

        /// <summary>
        /// Search count.
        /// </summary>
        public int SearchCount { get; set; }

        /// <summary>
        /// Skipped count for each schema type.
        /// </summary>
        public Dictionary<string, int> SkippedCount { get; set; }

        /// <summary>
        /// Processed count for each schema type.
        /// </summary>
        public Dictionary<string, int> ProcessedCount { get; set; }

        /// <summary>
        /// Has completed all resources.
        /// </summary>
        public bool IsCompleted { get; set; }

        public static TaskContext Create(
            string resourceType,
            Job job)
        {
            var isCompleted = job.CompletedResources.Contains(resourceType);

            var schemaTypes = new List<string>();
            var resourceProcessedCounts = new Dictionary<string, int>();
            var resourceSkippedCounts = new Dictionary<string, int>();
            var resourcePartIds = new Dictionary<string, int>();

            return new TaskContext(
                Guid.NewGuid().ToString("N"),
                job.Id,
                resourceType,
                schemaTypes,
                job.DataPeriod.Start,
                job.DataPeriod.End,
                job.ResourceProgresses[resourceType],
                resourceProcessedCounts,
                resourceSkippedCounts,
                resourcePartIds,
                job.TotalResourceCounts[resourceType],
                isCompleted);
        }
    }
}
