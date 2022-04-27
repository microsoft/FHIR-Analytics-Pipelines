// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Reflection.Metadata;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class JobConfiguration
    {
        /// <summary>
        /// Container name for this job.
        /// </summary>
        [JsonProperty("containerName")]
        public string ContainerName { get; set; }

        /// <summary>
        /// Start time of the job.
        /// </summary>
        [JsonProperty("startTime")]
        public DateTimeOffset StartTime { get; set; } = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>
        /// End time of the job.
        /// </summary>
        [JsonProperty("endTime")]
        public DateTimeOffset? EndTime { get; set; }

        /// <summary>
        /// Resource types to process.
        /// If no resources are present, we will process all resource types.
        /// </summary>
        [Obsolete("We recommend to use similiar type filters defined in FHIR $export operation.")]
        [JsonProperty("resourceTypeFilters")]
        public IEnumerable<string> ResourceTypeFilters { get; set; }

        [JsonProperty("jobType")]
        public ExportedType JobType { get; set; }

        /// <summary>
        /// Selected resource types, delimited by comma.
        /// </summary>
        [JsonProperty("_type")]
        public string SelectedTypes { get; set; }

        [JsonProperty("_typeFilters")]
        public string TypeFilters { get; set; }
    }
}
