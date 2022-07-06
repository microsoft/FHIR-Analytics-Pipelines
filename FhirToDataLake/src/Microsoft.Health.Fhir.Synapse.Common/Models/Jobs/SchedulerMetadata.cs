// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Jobs
{
    /// <summary>
    /// Scheduler settings stored in cloud store.
    /// Contains info for job scheduling.
    /// </summary>
    public class SchedulerMetadata
    {
        /// <summary>
        /// Last scheduled timestamp of the job.
        /// It is the end timestamp of the newest completed job.
        /// </summary>
        [JsonProperty("lastScheduledTimestamp")]
        public DateTimeOffset? LastScheduledTimestamp { get; set; }

        /// <summary>
        /// The patient id hashset which has been processed before.
        /// It is used to distinguish old and newly patients.
        /// </summary>
        [JsonProperty("processedPatientIds")]
        public IEnumerable<string> ProcessedPatientIds { get; set; } = new HashSet<string>();

        /// <summary>
        /// Scheduled jobs that have been stopped due to errors.
        /// New triggers will resume the execution.
        /// </summary>
        [JsonProperty("failedJobs")]
        public IEnumerable<Job> FailedJobs { get; set; } = new List<Job>();
    }
}
