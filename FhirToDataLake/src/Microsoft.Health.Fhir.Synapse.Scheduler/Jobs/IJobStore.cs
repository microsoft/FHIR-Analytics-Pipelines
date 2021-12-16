// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Jobs
{
    // ToDo: refine logic to multiple jobs
    public interface IJobStore
    {
        /// <summary>
        ///  List all active jobs from job store.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>job lists.</returns>
        public Task<IEnumerable<Job>> GetActiveJobsAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Update a job in store.
        /// </summary>
        /// <param name="job">the input job.</param>
        /// <param name="cancellationToken">the input cancellationToken.</param>
        /// <returns>Updated job.</returns>
        public Task<bool> UpdateJobAsync(Job job, CancellationToken cancellationToken = default);

        /// <summary>
        /// Complete the running job.
        /// It will first copy the job info to completed / failed store, then delete the running job instance.
        /// </summary>
        /// <param name="job">job to complete.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Completed job.</returns>
        public Task<bool> CompleteJobAsync(Job job, CancellationToken cancellationToken = default);

        /// <summary>
        /// Acquire job lock in job store.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>operation result.</returns>
        public Task<bool> AcquireJobLock(CancellationToken cancellationToken = default);

        /// <summary>
        /// Release job lock in job store.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>operation result.</returns>
        public Task<bool> ReleaseJobLock(CancellationToken cancellationToken = default);

        /// <summary>
        /// Get scheduler metadata from job store.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>SchedulerSetting object, return null if not exists.</returns>
        public Task<SchedulerMetadata> GetSchedulerMetadata(CancellationToken cancellationToken = default);

        /// <summary>
        /// Commit job data from staging folder to result folder.
        /// </summary>
        /// <param name="job">input job object.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>completed task.</returns>
        public Task CommitJobDataAsync(Job job, CancellationToken cancellationToken = default);
    }
}
