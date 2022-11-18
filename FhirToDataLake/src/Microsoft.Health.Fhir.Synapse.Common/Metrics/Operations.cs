// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class Operations
    {
        /// <summary>
        /// The stage to create job.
        /// </summary>
        public static string CreateJob => nameof(CreateJob);

        /// <summary>
        /// The stage to run job.
        /// </summary>
        public static string RunJob => nameof(RunJob);

        /// <summary>
        /// The stage of health check.
        /// </summary>
        public static string HealthCheck => nameof(HealthCheck);

        /// <summary>
        /// The stage of running scheduler service.
        /// </summary>
        public static string RunSchedulerService => nameof(RunSchedulerService);

        /// <summary>
        /// The stage of complete job.
        /// </summary>
        public static string CompleteJob => nameof(CompleteJob);
    }
}
