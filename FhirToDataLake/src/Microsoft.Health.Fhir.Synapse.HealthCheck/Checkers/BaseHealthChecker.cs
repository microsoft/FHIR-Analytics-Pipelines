// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public abstract class BaseHealthChecker : IHealthChecker
    {
        private readonly ILogger<BaseHealthChecker> _logger;

        protected BaseHealthChecker(
            string healthCheckName,
            bool isCritical,
            ILogger<BaseHealthChecker> logger)
        {
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            Name = EnsureArg.IsNotNullOrWhiteSpace(healthCheckName, nameof(healthCheckName));
            IsCritical = isCritical;
        }

        public string Name { get; set; }

        public bool IsCritical { get; set; } = true;

        public async Task<HealthCheckResult> PerformHealthCheckAsync(CancellationToken cancellationToken = default)
        {
            var healthCheckResult;
            var startTime = DateTimeOffset.UtcNow;
            try
            {
                healthCheckResult = await PerformHealthCheckImplAsync(cancellationToken);
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Unhandled exception : The component {Name} is not healthy.");
                healthCheckResult = new HealthCheckResult()
                {
                    Name = Name,
                    Status = HealthCheckStatus.UNHEALTHY,
                    IsCritical = IsCritical,
                    ErrorMessage = e.Message,
                    StartTime = startTime
                };
            }

            _logger.LogInformation($"Health check {Name} complete. Status {healthCheckResult.Status}");
            return healthCheckResult;
        }

        protected abstract Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken);
    }
}
