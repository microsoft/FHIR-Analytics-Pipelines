// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    public class TaskContext
    {
        // TODO: Refine this together with TaskExecutor class. Maybe this is more like a task class.
        public TaskContext(
            string id,
            int taskIndex,
            string jobId,
            FilterScope filterScope,
            DataPeriod dataPeriod,
            DateTimeOffset since,
            IList<TypeFilter> typeFilters,
            IList<PatientWrapper> patients = null,
            SearchProgress searchProgress = null,
            Dictionary<string, int> outputFileIndexMap = null,
            Dictionary<string, int> searchCount = null,
            Dictionary<string, int> processedCount = null,
            Dictionary<string, int> skippedCount = null,
            Dictionary<string, int> outputCount = null,
            Dictionary<string, long> outputDataSize = null,
            bool isCompleted = false)
        {
            // immutable fields
            Id = id;
            TaskIndex = taskIndex;
            JobId = jobId;
            FilterScope = filterScope;
            DataPeriod = dataPeriod;
            Since = since;
            TypeFilters = typeFilters;
            Patients = patients;

            // fields to record progress
            SearchProgress = searchProgress ?? new SearchProgress();
            OutputFileIndexMap = outputFileIndexMap ?? new Dictionary<string, int>();
            IsCompleted = isCompleted;

            // statistical fields
            SearchCount = searchCount ?? new Dictionary<string, int>();
            ProcessedCount = processedCount ?? new Dictionary<string, int>();
            SkippedCount = skippedCount ?? new Dictionary<string, int>();
            OutputCount = outputCount ?? new Dictionary<string, int>();
            OutputDataSize = outputDataSize ?? new Dictionary<string, long>();
        }

        /// <summary>
        /// Task id.
        /// </summary>
        [JsonProperty("id")]
        public string Id { get; }

        /// <summary>
        /// Task index
        /// </summary>
        [JsonProperty("taskIndex")]
        public int TaskIndex { get; }

        /// <summary>
        /// Job id.
        /// </summary>
        [JsonProperty("jobId")]
        public string JobId { get; set; }

        /// <summary>
        /// Filter Scope.
        /// </summary>
        [JsonProperty("filterScope")]
        public FilterScope FilterScope { get; }

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
        /// Type filter list.
        /// </summary>
        [JsonProperty("typeFilters")]
        public IList<TypeFilter> TypeFilters { get; }

        /// <summary>
        /// Patient id list, used in "Group" scope.
        /// </summary>
        [JsonProperty("patients")]
        public IList<PatientWrapper> Patients { get; }

        /// <summary>
        /// Search progress
        /// </summary>
        [JsonProperty("searchProgress")]
        public SearchProgress SearchProgress { get; set; }

        /// <summary>
        /// Output file index map for all resources/schemas.
        /// The value of each schemaType will be appended to output files.
        /// The format is '{SchemaType}_{TaskIndex:d10}_{index:d10}.parquet', e.g. Patient_0000000001_0000000001.parquet.
        /// </summary>
        [JsonProperty("outputFileIndexMap")]
        public Dictionary<string, int> OutputFileIndexMap { get; set; }

        /// <summary>
        /// Search count for each resource type.
        /// </summary>
        [JsonProperty("searchCount")]
        public Dictionary<string, int> SearchCount { get; set; }

        /// <summary>
        /// Output count for each resource type.
        /// </summary>
        [JsonProperty("outputCount")]
        public Dictionary<string, int> OutputCount { get; set; }

        /// <summary>
        /// Output data size for each resource type.
        /// </summary>
        [JsonProperty("outputDataSize")]
        public Dictionary<string, long> OutputDataSize { get; set; }

        /// <summary>
        /// Skipped count for each schema type.
        /// </summary>
        [JsonProperty("skippedCount")]
        public Dictionary<string, int> SkippedCount { get; set; }

        /// <summary>
        /// Processed count for each schema type.
        /// </summary>
        [JsonProperty("processedCount")]
        public Dictionary<string, int> ProcessedCount { get; set; }

        /// <summary>
        /// Is this task completed.
        /// </summary>
        [JsonProperty("isCompleted")]
        public bool IsCompleted { get; set; }

        public static TaskContext CreateFromJob(
            Job job,
            IList<TypeFilter> typeFilters,
            IList<PatientWrapper> patients = null)
        {
            return new TaskContext(
                Guid.NewGuid().ToString("N"),
                job.NextTaskIndex,
                job.Id,
                job.FilterInfo.FilterScope,
                job.DataPeriod,
                job.FilterInfo.Since,
                typeFilters,
                patients);
        }
    }
}
