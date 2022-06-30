// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Jobs
{
    public class Job
    {
        public Job(
            string id,
            string containerName,
            JobStatus status,
            DataPeriod dataPeriod,
            DateTimeOffset since,
            FilterContext filterContext,
            DateTimeOffset lastHeartBeat,
            DateTimeOffset createdTime,
            IEnumerable<PatientWrapper> patients = null,
            int nextTaskIndex = 0,
            Dictionary<string, TaskContext> runningTasks = null,
            Dictionary<string, int> totalResourceCounts = null,
            Dictionary<string, int> processedResourceCounts = null,
            Dictionary<string, int> skippedResourceCounts = null,
            string resumedJobId = null)
        {
            Id = id;
            CreatedTime = createdTime;
            ContainerName = containerName;
            Status = status;
            DataPeriod = dataPeriod;
            Since = since;
            FilterContext = filterContext;

            LastHeartBeat = lastHeartBeat;

            Patients = patients ?? new List<PatientWrapper>();

            // fields to record job progress
            NextTaskIndex = nextTaskIndex;
            RunningTasks = runningTasks ?? new Dictionary<string, TaskContext>();
            TotalResourceCounts = totalResourceCounts ?? new Dictionary<string, int>();
            ProcessedResourceCounts = processedResourceCounts ?? new Dictionary<string, int>();
            SkippedResourceCounts = skippedResourceCounts ?? new Dictionary<string, int>();

            ResumedJobId = resumedJobId;
        }

        /// <summary>
        /// Job id.
        /// </summary>
        [JsonProperty("id")]
        public string Id { get; }

        /// <summary>
        /// Created timestamp of job.
        /// </summary>
        [JsonProperty("createdTime")]
        public DateTimeOffset CreatedTime { get; }

        /// <summary>
        /// Completed timestamp of job.
        /// </summary>
        [JsonProperty("completedTime")]
        public DateTimeOffset? CompletedTime { get; set; }

        /// <summary>
        /// Container name.
        /// </summary>
        [JsonProperty("containerName")]
        public string ContainerName { get; }

        /// <summary>
        /// Job status.
        /// </summary>
        [JsonProperty("status")]
        public JobStatus Status { get; set; }

        /// <summary>
        /// Will process all data with timestamp greater than or equal to DataPeriod.Start and less than DataPeriod.End.
        /// For FHIR data, we use the lastUpdated field as the record timestamp.
        /// </summary>
        [JsonProperty("dataPeriod")]
        public DataPeriod DataPeriod { get; }

        /// <summary>
        /// The start timestamp specified in job configuration.
        /// </summary>
        [JsonProperty("since")]
        public DateTimeOffset Since { get; }

        /// <summary>
        /// The filter context.
        /// </summary>
        [JsonProperty("filterContext")]
        public FilterContext FilterContext { get; }

        /// <summary>
        /// Patient list
        /// </summary>
        [JsonProperty("patients")]
        public IEnumerable<PatientWrapper> Patients { get; set; }

        /// <summary>
        /// Gets or sets last heartbeat timestamp of job.
        /// </summary>
        [JsonProperty("lastHeartBeat")]
        public DateTimeOffset LastHeartBeat { get; set; }

        /// <summary>
        /// The next task index
        /// </summary>
        [JsonProperty("nextTaskIndex")]
        public int NextTaskIndex { get; set; }

        /// <summary>
        /// The running tasks
        /// </summary>
        [JsonProperty("runningTasks")]
        public Dictionary<string, TaskContext> RunningTasks { get; set; }

        /// <summary>
        /// Total resource count (from data source) for each resource types.
        /// </summary>
        [JsonProperty("totalResourceCounts")]
        public Dictionary<string, int> TotalResourceCounts { get; set; }

        /// <summary>
        /// Processed resource count for each schema type.
        /// </summary>
        [JsonProperty("processedResourceCounts")]
        public Dictionary<string, int> ProcessedResourceCounts { get; set; }

        /// <summary>
        /// Skipped resource count for each schema type.
        /// </summary>
        [JsonProperty("skippedResourceCounts")]
        public Dictionary<string, int> SkippedResourceCounts { get; set; }

        /// <summary>
        /// Failure reason for this job.
        /// </summary>
        [JsonProperty("failedReason")]
        public string FailedReason { get; set; }

        /// <summary>
        /// Id of the resumed job.
        /// </summary>
        [JsonProperty("ResumedJobId")]
        public string ResumedJobId { get; }

        public static Job Create(
            string containerName,
            JobStatus status,
            DataPeriod dataPeriod,
            DateTimeOffset since,
            FilterContext filterContext,
            IEnumerable<PatientWrapper> patients = null,
            int nextTaskIndex = 0,
            Dictionary<string, TaskContext> runningTasks = null,
            Dictionary<string, int> totalResourceCounts = null,
            Dictionary<string, int> processedResourceCounts = null,
            Dictionary<string, int> skippedResourceCounts = null,
            string resumedJobId = null)
        {
            return new Job(
                Guid.NewGuid().ToString("N"),
                containerName,
                status,
                dataPeriod,
                since,
                filterContext,
                DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow,
                patients,
                nextTaskIndex,
                runningTasks,
                totalResourceCounts,
                processedResourceCounts,
                skippedResourceCounts,
                resumedJobId);
        }
    }
}
