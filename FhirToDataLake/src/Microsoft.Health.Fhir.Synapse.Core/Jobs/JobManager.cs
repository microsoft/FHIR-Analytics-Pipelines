// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobManager
    {
        private readonly JobHosting _jobHosting;
        private readonly ISchedulerService _scheduler;
        private readonly JobConfiguration _jobConfiguration;
        private readonly ILogger<JobManager> _logger;

        public JobManager(
            JobHosting jobHosting,
            ISchedulerService schedulerService,
            IOptions<JobConfiguration> jobConfiguration,
            ILogger<JobManager> logger)
        {
            _jobHosting = EnsureArg.IsNotNull(jobHosting, nameof(jobHosting));
            _scheduler = EnsureArg.IsNotNull(schedulerService, nameof(schedulerService));
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            _jobConfiguration = jobConfiguration.Value;
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        /// <summary>
        /// Start to run scheduler service and job hosting.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Completed task.</returns>
        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Job manager starts running.");

            using var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var scheduleTask = _scheduler.RunAsync(cancellationTokenSource.Token);

            // TODO: the max running job count need to be decided after performance test
            _jobHosting.MaxRunningJobCount = (short)_jobConfiguration.MaxRunningJobCount;
            await _jobHosting.StartAsync((byte)_jobConfiguration.QueueType, Environment.MachineName, cancellationTokenSource);
            await scheduleTask;
        }
    }
}