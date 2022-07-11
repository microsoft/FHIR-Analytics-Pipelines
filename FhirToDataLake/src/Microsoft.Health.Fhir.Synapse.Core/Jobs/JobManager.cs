// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AzureTableTaskQueue.Synapse;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobManager
    {
        private readonly JobHosting _jobHosting;
        private readonly SchedulerService _scheduler;
        private readonly JobConfiguration _jobConfiguration;
        private readonly ILogger<JobManager> _logger;

        public JobManager(
            JobHosting jobHosting,
            SchedulerService scheduler,
            IOptions<JobConfiguration> jobConfiguration,
            ILogger<JobManager> logger)
        {
            _jobHosting = jobHosting;
            _scheduler = scheduler;
            _jobConfiguration = jobConfiguration.Value;
            _logger = logger;
        }

        /// <summary>
        /// Resume an active job or trigger new job from job store.
        /// and execute the job.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Completed task.</returns>
        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            using CancellationTokenSource cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var scheduleTask = _scheduler.ExecuteAsync(cancellationTokenSource.Token);
            _jobHosting.MaxRunningJobCount = 5;
            await _jobHosting.StartAsync(_jobConfiguration.QueueType, Environment.MachineName, cancellationTokenSource);
            await scheduleTask;
        }
    }
}
