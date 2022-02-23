// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    // ToDo: refine logic to multiple jobs
    public interface IJobStore : IDisposable
    {
        /// <summary>
        ///  Acquire an active job from job store.
        ///  If no active job found, will return null.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>An active job or null.</returns>
        public Task<Job> AcquireActiveJobAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Update a job in store.
        /// </summary>
        /// <param name="job">the input job.</param>
        /// <param name="cancellationToken">the input cancellationToken.</param>
        /// <returns>Completed task.</returns>
        public Task UpdateJobAsync(Job job, CancellationToken cancellationToken = default);

        /// <summary>
        /// Complete the running job.
        /// It will first copy the job info to completed / failed store, then delete the running job instance.
        /// </summary>
        /// <param name="job">job to complete.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Completed task.</returns>
        public Task CompleteJobAsync(Job job, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get scheduler metadata from job store.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>SchedulerMetadata object, return null if not exists.</returns>
        public Task<SchedulerMetadata> GetSchedulerMetadataAsync(CancellationToken cancellationToken = default);
    }
}
