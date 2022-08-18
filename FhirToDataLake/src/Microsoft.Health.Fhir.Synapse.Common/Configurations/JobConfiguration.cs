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
        // TODO: agent name is used as part of table name and queue name, need to validate agent name to contains only alphanumeric characters, and not begin with a numeric character.
        // Reference: https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#table-names
        // https://docs.microsoft.com/en-us/rest/api/storageservices/naming-queues-and-metadata#queue-names
        [JsonProperty("agentName")]
        public string AgentName { get; set; }

        /// <summary>
        /// Container name for this job.
        /// </summary>
        // TODO: will be removed when generic task is enabled
        [JsonProperty("containerName")]
        public string ContainerName { get; set; }

        /// <summary>
        /// Start time of the job.
        /// </summary>
        // TODO: will be removed, disable start time
        [JsonProperty("startTime")]
        public DateTimeOffset StartTime { get; set; } = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>
        /// End time of the job.
        /// </summary>
        // TODO: will be removed, disable end time, the agent is a long running service
        [JsonProperty("endTime")]
        public DateTimeOffset? EndTime { get; set; }
    }
}
