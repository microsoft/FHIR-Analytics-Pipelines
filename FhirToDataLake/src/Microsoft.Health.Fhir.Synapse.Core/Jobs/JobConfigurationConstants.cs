// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public static class JobConfigurationConstants
    {
        // scheduler service configurations

        /// <summary>
        /// For each triggered job, we will query all FHIR data in a certain time period.
        /// But when the end time of a period is very close to utcNow,
        /// we have a risk to lose data that have not been saved to FHIR DB.
        /// So we set a latency to query FHIR data.
        /// </summary>
        public const int JobQueryLatencyInMinutes = 2;

        /// <summary>
        /// The pulling interval time in seconds for scheduler service
        /// </summary>
        public const int DefaultSchedulerServicePullingIntervalInSeconds = 20;

        /// <summary>
        /// Expiration time span for scheduler service lock.
        /// </summary>
        public const int DefaultSchedulerServiceLeaseExpirationInSeconds = 180;

        /// <summary>
        /// Time interval to refresh scheduler service lock lease.
        /// </summary>
        public const int DefaultSchedulerServiceLeaseRefreshIntervalInSeconds = 60;

        // orchestrator job configurations

        /// <summary>
        /// The time interval in seconds of each processing job for initial orchestrator job, used in system filter scope.
        /// </summary>
        public const int DefaultInitialOrchestrationIntervalInSeconds = 1800;

        /// <summary>
        /// The time interval in seconds of each processing job for incremental orchestrator job, used in system filter scope.
        /// </summary>
        public const int DefaultIncrementalOrchestrationIntervalInSeconds = 60;

        /// <summary>
        /// The number of patients in each processing job, used in group filter scope.
        /// </summary>
        public const int DefaultNumberOfPatientsPerProcessingJob = 100;

        /// <summary>
        /// The time interval in seconds to check processing job status in orchestrator job.
        /// </summary>
        public const int DefaultCheckFrequencyInSeconds = 10;

        // processing job configurations

        /// <summary>
        /// The cache resources number, will commit the cache to storage if there are more resources than this value in cache.
        /// </summary>
        public const int NumberOfResourcesPerCommit = 5000;

        /// <summary>
        /// The data size of cache in bytes, will commit the cache to storage it the data size is larger than this value.
        /// </summary>
        public const int DataSizeInBytesPerCommit = 5 * 1024 * 1024;

        /// <summary>
        /// The running job count threshold to check running job complete
        /// </summary>
        public const int CheckRunningJobCompleteRunningJobCountThreshold = 5;
    }
}