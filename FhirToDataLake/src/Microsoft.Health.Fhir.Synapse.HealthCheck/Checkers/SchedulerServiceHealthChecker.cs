// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public class SchedulerServiceHealthChecker : BaseHealthChecker
    {
        private readonly ISchedulerService _schedulerService;
        private const int _schedulerServiceInActiveTimeInterval = 300;

        public SchedulerServiceHealthChecker(
            ISchedulerService schedulerService,
            ILogger<SchedulerServiceHealthChecker> logger)
            : base(HealthCheckTypes.SchedulerServiceIsActive, true, logger)
        {
            _schedulerService = EnsureArg.IsNotNull(schedulerService, nameof(schedulerService));
        }

        protected override Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            var healthCheckResult = new HealthCheckResult(HealthCheckTypes.SchedulerServiceIsActive, true);

            if (_schedulerService.LastHeartbeat.AddSeconds(_schedulerServiceInActiveTimeInterval) < DateTimeOffset.UtcNow)
            {
                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = $"Scheduler service is inactive.";
            }

            return Task.FromResult(healthCheckResult);
        }
    }
}
