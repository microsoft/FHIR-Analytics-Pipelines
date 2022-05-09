// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Jobs
{
    public class Job
    {
        public Job(
            string containerName,
            JobStatus status,
            IEnumerable<string> resourceTypes,
            DataPeriod dataPeriod,
            DateTimeOffset lastHeartBeat,
            Dictionary<string, List<string>> schemaTypesMap = null,
            Dictionary<string, string> resourceProgresses = null,
            Dictionary<string, int> totalResourceCounts = null,
            Dictionary<string, int> processedResourceCounts = null,
            Dictionary<string, int> skippedResourceCounts = null,
            Dictionary<string, int> partIds = null,
            HashSet<string> completedResources = null,
            string resumedJobId = null)
        {
            Id = Guid.NewGuid().ToString("N");
            CreatedTime = DateTimeOffset.UtcNow;
            ContainerName = containerName;
            Status = status;
            ResourceTypes = resourceTypes ?? new List<string>();
            DataPeriod = dataPeriod;
            LastHeartBeat = lastHeartBeat;
            SchemaTypesMap = schemaTypesMap ?? new Dictionary<string, List<string>>();
            ResourceProgresses = resourceProgresses ?? new Dictionary<string, string>();
            TotalResourceCounts = totalResourceCounts ?? new Dictionary<string, int>();
            ProcessedResourceCounts = processedResourceCounts ?? new Dictionary<string, int>();
            SkippedResourceCounts = skippedResourceCounts ?? new Dictionary<string, int>();
            PartIds = partIds ?? new Dictionary<string, int>();
            CompletedResources = completedResources ?? new HashSet<string>();
            ResumedJobId = resumedJobId;

            foreach (var resource in ResourceTypes)
            {
                _ = ResourceProgresses.TryAdd(resource, null);
                _ = TotalResourceCounts.TryAdd(resource, 0);

                // Temporarily set schema type list only contains single value for each resource type
                // E.g:
                // Schema list for "Patient" resource is ["Patient"]
                // Schema list for "Observation" resource is ["Observation"]
                _ = SchemaTypesMap.TryAdd(resource, new List<string>() { resource });

                // After supporting customized schema, the schema type map will be set below when customized schema is enable
                // _ = SchemaTypesMap.TryAdd(resource, new List<string>() { resource, $"{resource}_customized"});
            }

            foreach (var resource in ResourceTypes)
            {
                foreach (var schemaType in SchemaTypesMap[resource])
                {
                    _ = ProcessedResourceCounts.TryAdd(schemaType, 0);
                    _ = SkippedResourceCounts.TryAdd(schemaType, 0);
                    _ = PartIds.TryAdd(schemaType, 0);
                }
            }
        }

        /// <summary>
        /// Job id.
        /// </summary>
        [JsonProperty("id")]
        public string Id { get; set; }

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
        /// All resource types to process.
        /// </summary>
        [JsonProperty("resourceTypes")]
        public IEnumerable<string> ResourceTypes { get; set; }

        /// <summary>
        /// Schema types for each resource type.
        /// </summary>
        [JsonProperty("schemaTypesMap")]
        public Dictionary<string, List<string>> SchemaTypesMap { get; set; }

        /// <summary>
        /// Will process all data with timestamp greater than or equal to DataPeriod.Start and less than DataPeriod.End.
        /// For FHIR data, we use the lastUpdated field as the record timestamp.
        /// </summary>
        [JsonProperty("dataPeriod")]
        public DataPeriod DataPeriod { get; }

        /// <summary>
        /// Gets or sets last heartbeat timestamp of job.
        /// </summary>
        [JsonProperty("lastHeartBeat")]
        public DateTimeOffset LastHeartBeat { get; set; }

        /// <summary>
        /// Gets or sets resource progresses (continuation tokens for search) of job.
        /// </summary>
        [JsonProperty("resourceProgresses")]
        public Dictionary<string, string> ResourceProgresses { get; }

        /// <summary>
        /// Total resource count (from data source) for each resource types.
        /// </summary>
        [JsonProperty("totalResourceCounts")]
        public Dictionary<string, int> TotalResourceCounts { get; }

        /// <summary>
        /// Processed resource count for each resource type.
        /// </summary>
        [JsonProperty("processedResourceCounts")]
        public Dictionary<string, int> ProcessedResourceCounts { get; }

        /// <summary>
        /// Skipped resource count for each resource type.
        /// </summary>
        [JsonProperty("skippedResourceCounts")]
        public Dictionary<string, int> SkippedResourceCounts { get; }

        /// <summary>
        /// Resource set that has completed processing.
        /// </summary>
        [JsonProperty("completedResources")]
        public HashSet<string> CompletedResources { get; }

        /// <summary>
        /// Part id for each resource type.
        /// </summary>
        [JsonProperty("partIds")]
        public Dictionary<string, int> PartIds { get; }

        /// <summary>
        /// Failure reason for this job.
        /// </summary>
        [JsonProperty("failedReason")]
        public string FailedReason { get; set; }

        /// <summary>
        /// Id of the resumed job.
        /// </summary>
        [JsonProperty("ResumedJobId")]
        public string ResumedJobId { get; set; }
    }
}
