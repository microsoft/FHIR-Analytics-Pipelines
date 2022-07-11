// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class JobConfiguration
    {
        [JsonProperty("queueType")]
        public byte QueueType { get; set; }

        [JsonProperty("tableUrl")]
        public string TableUrl { get; set; }

        [JsonProperty("queueUrl")]
        public string QueueUrl { get; set; }

        [JsonProperty("schedulerCronExpression")]
        public string SchedulerCronExpression { get; set; }

        [JsonProperty("agentId")]
        public string AgentId { get; set; }

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
        [JsonProperty("resourceTypeFilters")]
        public IEnumerable<string> ResourceTypeFilters { get; set; }
    }
}
