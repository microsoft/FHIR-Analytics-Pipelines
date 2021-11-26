// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Jobs
{
    public static class JobConstants
    {
        public const string JobConfigName = "jobs/job.config.json";

        public const string RunningJobName = "jobs/running.job.json";

        public const string CompletedJobFolder = "jobs/completedJobs";

        public const string FailedJobFolder = "jobs/failedJobs";

        public const int JobActiveThresholdInMinutes = 10;

        public const int JobEndTimeLatencyInMinutes = 2;
    }
}
