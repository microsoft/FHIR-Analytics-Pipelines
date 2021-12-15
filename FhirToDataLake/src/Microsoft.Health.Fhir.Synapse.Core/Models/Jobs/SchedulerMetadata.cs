// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Models.Jobs
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
    }
}
