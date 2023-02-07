// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.AnalyticsConnector.Common.Models.Jobs;
using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations
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
        /// Job information table name.
        /// </summary>
        [JsonProperty("jobInfoTableName")]
        public string JobInfoTableName { get; set; }

        /// <summary>
        /// Metadata table name.
        /// </summary>
        [JsonProperty("metadataTableName")]
        public string MetadataTableName { get; set; }

        /// <summary>
        /// Job information queue name.
        /// </summary>
        [JsonProperty("jobInfoQueueName")]
        public string JobInfoQueueName { get; set; }

        /// <summary>
        /// Scheduler crontab expression.
        /// </summary>
        [JsonProperty("schedulerCronExpression")]
        public string SchedulerCronExpression { get; set; }

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

        /// <summary>
        /// Max running job count in an instance.
        /// </summary>
        [JsonProperty("maxRunningJobCount")]
        public int MaxRunningJobCount { get; set; } = 3;

        /// <summary>
        /// Max queued job count per orchestration job.
        /// </summary>
        [JsonProperty("maxQueuedJobCountPerOrchestration")]
        public int MaxQueuedJobCountPerOrchestration { get; set; } = 100;
    }
}
