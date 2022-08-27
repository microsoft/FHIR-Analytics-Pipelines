// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class JobConfiguration
    {
        /// <summary>
        /// Queue type
        /// </summary>
        [JsonProperty("queueType")]
        public QueueType QueueType { get; set; }

        /// <summary>
        /// Table Url
        /// </summary>
        [JsonProperty("tableUrl")]
        public string TableUrl { get; set; }

        /// <summary>
        /// Queue Url
        /// </summary>
        [JsonProperty("queueUrl")]
        public string QueueUrl { get; set; }

        /// <summary>
        /// Scheduler crontab expression.
        /// </summary>
        [JsonProperty("schedulerCronExpression")]
        public string SchedulerCronExpression { get; set; }

        /// <summary>
        /// Agent name
        /// </summary>
        [JsonProperty("agentName")]
        public string AgentName { get; set; }

        /// <summary>
        /// Container name for this job.
        /// </summary>
        [JsonProperty("containerName")]
        public string ContainerName { get; set; }

        /// <summary>
        /// Start time of the job.
        /// </summary>
        // TODO: don't expose to PaaS
        [JsonProperty("startTime")]
        public DateTimeOffset? StartTime { get; set; }

        /// <summary>
        /// End time of the job.
        /// </summary>
        // TODO: don't expose to PaaS
        [JsonProperty("endTime")]
        public DateTimeOffset? EndTime { get; set; }
    }
}
