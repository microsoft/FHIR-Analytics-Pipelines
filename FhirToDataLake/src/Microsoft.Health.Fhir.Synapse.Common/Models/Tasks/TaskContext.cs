// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
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
            DateTimeOffset startTime,
            DateTimeOffset endTime,
            string continuationToken,
            int searchCount = 0,
            int processedCount = 0,
            int skippedCount = 0,
            int partId = 0,
            bool isCompleted = false)
        {
            Id = id;
            JobId = jobId;
            ResourceType = resourceType;
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
        /// Start time for task.
        /// </summary>
        public DateTimeOffset StartTime { get; set; }

        /// <summary>
        /// End time for task.
        /// </summary>
        public DateTimeOffset EndTime { get; set; }

        /// <summary>
        /// Part id for task output files.
        /// The format is '{Resource}_{JobId}_{PartId}.parquet', e.g. Patient_1ab3edcefsi789ed_0001.parquet.
        /// </summary>
        public int PartId { get; set; }

        /// <summary>
        /// Task progress.
        /// </summary>
        public string ContinuationToken { get; set; }

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
        /// Has completed all resources.
        /// </summary>
        public bool IsCompleted { get; set; }

        public static TaskContext Create(
            string resourceType,
            Job job)
        {
            var isCompleted = job.CompletedResources.Contains(resourceType);

            return new TaskContext(
                Guid.NewGuid().ToString("N"),
                job.Id,
                resourceType,
                job.DataPeriod.Start,
                job.DataPeriod.End,
                job.ResourceProgresses[resourceType],
                job.TotalResourceCounts[resourceType],
                job.ProcessedResourceCounts[resourceType],
                job.SkippedResourceCounts[resourceType],
                job.PartIds[resourceType],
                isCompleted);
        }
    }
}
