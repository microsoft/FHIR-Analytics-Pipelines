// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Jobs
{
    // ToDo: refine logic to multiple jobs
    public interface IJobStore
    {
        /// <summary>
        /// Create or resume job from job store.
        /// </summary>
        /// <param name="cancellationToken">the input cancellationToken.</param>
        /// <returns>Created job.</returns>
        public Task<Job> StartJobAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Update a job in store.
        /// </summary>
        /// <param name="job">the 0 job.</param>
        /// <param name="cancellationToken">the input cancellationToken.</param>
        /// <returns>Updated job.</returns>
        public Task<Job?> UpdateJobAsync(Job job, CancellationToken cancellationToken = default);

        /// <summary>
        /// Complete the running job.
        /// It will first copy the job info to completed / failed store, then delete the running job instance.
        /// </summary>
        /// <param name="job">job to complete.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Completed job.</returns>
        public Task<Job> CompleteJobAsync(Job job, CancellationToken cancellationToken = default);

        /// <summary>
        /// Release the current running job. Used for application graceful exit.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Operation result.</returns>
        public Task<bool> ReleaseJobAsync(CancellationToken cancellationToken = default);
    }
}
