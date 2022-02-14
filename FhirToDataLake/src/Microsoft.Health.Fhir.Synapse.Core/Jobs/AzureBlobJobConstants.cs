// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public static class AzureBlobJobConstants
    {
        /// <summary>
        /// Blob file storing information for scheduler status.
        /// Including job scheduling statuses.
        /// </summary>
        public const string SchedulerMetadataFileName = "jobs/scheduler.metadata";

        /// <summary>
        /// Lock blob for job synchronization to avoid petential race conditions.
        /// </summary>
        public const string JobLockFileName = "jobs/.job.lock";

        /// <summary>
        /// Blob folder for active jobs.
        /// </summary>
        public const string ActiveJobFolder = "jobs/activeJobs";

        /// <summary>
        /// Blob folder for succeeded jobs.
        /// </summary>
        public const string CompletedJobFolder = "jobs/completedJobs";

        /// <summary>
        /// Blob folder for failed jobs.
        /// </summary>
        public const string FailedJobFolder = "jobs/failedJobs";

        /// <summary>
        /// For each triggered job, we will query all FHIR data in a certain time period.
        /// But when the end time of a period is very close to utcNow,
        /// we have a risk to lose data that have not been saved to FHIR DB.
        /// So we set a latency to query FHIR data.
        /// </summary>
        public const int JobQueryLatencyInMinutes = 2;

        /// <summary>
        /// Expiration time span for job lock.
        /// </summary>
        public const int JobLeaseExpirationInSeconds = 30;

        /// <summary>
        /// Time interval to refresh job lock lease.
        /// </summary>
        public const int JobLeaseRefreshIntervalInSeconds = 20;
    }
}
