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
            ILogger<BaseHealthChecker> logger)
        {
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            Name = EnsureArg.IsNotNullOrWhiteSpace(healthCheckName, nameof(healthCheckName));
        }

        public string Name { get; set; }

        public async Task<HealthCheckResult> PerformHealthCheckAsync(CancellationToken cancellationToken = default)
        {
            var healthCheckResult = new HealthCheckResult(Name);

            try
            {
                await PerformHealthCheckImplAsync(healthCheckResult, cancellationToken);
                healthCheckResult.Status = HealthCheckStatus.PASS;
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"The component {Name} is not healthy.");
                healthCheckResult.Status = HealthCheckStatus.FAIL;
                healthCheckResult.ErrorMessage = e.Message;
            }

            _logger.LogInformation($"Health check {Name} complete. Status {healthCheckResult.Status}");
            return healthCheckResult;
        }

        protected abstract Task PerformHealthCheckImplAsync(HealthCheckResult healthCheckResult, CancellationToken cancellationToken);
    }
}
