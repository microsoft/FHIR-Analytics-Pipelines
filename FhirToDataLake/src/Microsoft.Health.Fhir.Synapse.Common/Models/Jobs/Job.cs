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
            FilterInfo filterInfo,
            DateTimeOffset createdTime,
            DateTimeOffset lastHeartBeat,
            IEnumerable<PatientWrapper> patients = null,
            int nextTaskIndex = 0,
            Dictionary<string, TaskContext> runningTasks = null,
            Dictionary<string, int> totalResourceCounts = null,
            Dictionary<string, int> processedResourceCounts = null,
            Dictionary<string, int> skippedResourceCounts = null,
            string resumedJobId = null)
        {
            // immutable fields
            Id = id;
            CreatedTime = createdTime;
            ContainerName = containerName;
            DataPeriod = dataPeriod;
            FilterInfo = filterInfo;
            ResumedJobId = resumedJobId;

            // fields will be assigned in the process of job executing
            Patients = patients ?? new List<PatientWrapper>();

            // fields to record job status
            // the following two fields "CompletedTime" and "FailedReason" are set when a job is completed
            Status = status;
            LastHeartBeat = lastHeartBeat;

            // fields to record job progress
            NextTaskIndex = nextTaskIndex;
            RunningTasks = runningTasks ?? new Dictionary<string, TaskContext>();

            // statistical fields
            TotalResourceCounts = totalResourceCounts ?? new Dictionary<string, int>();
            ProcessedResourceCounts = processedResourceCounts ?? new Dictionary<string, int>();
            SkippedResourceCounts = skippedResourceCounts ?? new Dictionary<string, int>();
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
        /// Will process all data with timestamp greater than or equal to DataPeriod.Start and less than DataPeriod.End.
        /// For FHIR data, we use the lastUpdated field as the record timestamp.
        /// </summary>
        [JsonProperty("dataPeriod")]
        public DataPeriod DataPeriod { get; }

        /// <summary>
        /// The filter information.
        /// </summary>
        [JsonProperty("filterInfo")]
        public FilterInfo FilterInfo { get; }

        /// <summary>
        /// Id of the resumed job.
        /// </summary>
        [JsonProperty("ResumedJobId")]
        public string ResumedJobId { get; }

        /// <summary>
        /// Job status.
        /// </summary>
        [JsonProperty("status")]
        public JobStatus Status { get; set; }

        /// <summary>
        /// Patient list to be processed, which are extracted from group member
        /// </summary>
        [JsonProperty("patients")]
        public IEnumerable<PatientWrapper> Patients { get; set; }

        /// <summary>
        /// Gets or sets last heartbeat timestamp of job.
        /// </summary>
        [JsonProperty("lastHeartBeat")]
        public DateTimeOffset LastHeartBeat { get; set; }

        /// <summary>
        /// Failure reason for this job.
        /// </summary>
        [JsonProperty("failedReason")]
        public string FailedReason { get; set; }

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

        public static Job Create(
            string containerName,
            JobStatus status,
            DataPeriod dataPeriod,
            FilterInfo filterInfo,
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
                filterInfo,
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
