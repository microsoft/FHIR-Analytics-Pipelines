// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public class JobManager
    {
        private readonly JobHosting _jobHosting;
        private readonly ISchedulerService _scheduler;
        private readonly IExternalDependencyChecker _externalDependencyChecker;
        private readonly JobConfiguration _jobConfiguration;
        private readonly ILogger<JobManager> _logger;

        private const int CheckExternalDependencyIntervalInSeconds = 30;

        public JobManager(
            JobHosting jobHosting,
            ISchedulerService schedulerService,
            IExternalDependencyChecker externalDependencyChecker,
            IOptions<JobConfiguration> jobConfiguration,
            ILogger<JobManager> logger)
        {
            _jobHosting = EnsureArg.IsNotNull(jobHosting, nameof(jobHosting));
            _scheduler = EnsureArg.IsNotNull(schedulerService, nameof(schedulerService));
            _externalDependencyChecker =
                EnsureArg.IsNotNull(externalDependencyChecker, nameof(externalDependencyChecker));
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
            _logger.LogInformation($"Job manager starts running with version {Assembly.GetExecutingAssembly().GetName().Version}");

            using var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            Task scheduleTask = _scheduler.RunAsync(cancellationTokenSource.Token);

            // wait until external dependencies are all ready
            while (!await _externalDependencyChecker.IsExternalDependencyReady(cancellationTokenSource.Token))
            {
                await Task.Delay(TimeSpan.FromSeconds(CheckExternalDependencyIntervalInSeconds), cancellationTokenSource.Token);
            }

            // TODO: the max running job count need to be decided after performance test
            _jobHosting.MaxRunningJobCount = (short)_jobConfiguration.MaxRunningJobCount;
            await _jobHosting.StartAsync((byte)_jobConfiguration.QueueType, Environment.MachineName, cancellationTokenSource);
            await scheduleTask;
        }
    }
}