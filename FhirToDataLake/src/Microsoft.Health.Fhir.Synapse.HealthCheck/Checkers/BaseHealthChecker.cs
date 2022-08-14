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
            var healthCheckResult = new HealthCheckResult(Name, IsCritical);

            try
            {
                await PerformHealthCheckImplAsync(cancellationToken);
                healthCheckResult.Status = HealthCheckStatus.HEALTHY;
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"The component {Name} is not healthy.");
                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = e.Message;
            }

            _logger.LogInformation($"Health check {Name} complete. Status {healthCheckResult.Status}");
            return healthCheckResult;
        }

        protected abstract Task PerformHealthCheckImplAsync(CancellationToken cancellationToken);
    }
}
