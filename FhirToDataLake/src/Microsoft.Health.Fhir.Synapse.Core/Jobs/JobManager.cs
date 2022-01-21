// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobManager
    {
        private readonly IJobStore _jobStore;
        private readonly JobExecutor _jobExecutor;
        private readonly ILogger<JobManager> _logger;

        public JobManager(
            IJobStore jobStore,
            JobExecutor jobExecutor,
            ILogger<JobManager> logger)
        {
            EnsureArg.IsNotNull(jobStore, nameof(jobStore));
            EnsureArg.IsNotNull(jobExecutor, nameof(jobExecutor));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _jobStore = jobStore;
            _jobExecutor = jobExecutor;

            _logger = logger;
        }

        /// <summary>
        /// Resume an active job or trigger new job from job store
        /// and execute the job.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Completed task.</returns>
        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            // Acquire lock to ensure the job store is changed from only one client.
            var job = await _jobStore.AcquireJobAsync(cancellationToken);

            try
            {
                await _jobExecutor.ExecuteAsync(job, cancellationToken);
                job.Status = JobStatus.Succeeded;
                await _jobStore.CompleteJobAsync(job, cancellationToken);
            }
            catch (Exception exception)
            {
                job.Status = JobStatus.Failed;
                job.FailedReason = exception.ToString();
                await _jobStore.UpdateJobAsync(job, cancellationToken);
                _logger.LogError(exception, "Process job '{jobId}' failed.", job.Id);
                throw;
            }
        }
    }
}
