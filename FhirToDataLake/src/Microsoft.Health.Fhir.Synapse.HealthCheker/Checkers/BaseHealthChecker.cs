// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Checkers
{
    public abstract class BaseHealthChecker : IHealthChecker
    {
        protected BaseHealthChecker(
            string healthCheckName,
            ILogger<BaseHealthChecker> logger)
        {
            Logger = EnsureArg.IsNotNull(logger, nameof(logger));
            Name = EnsureArg.IsNotEmptyOrWhiteSpace(healthCheckName, nameof(healthCheckName));
        }

        protected ILogger<BaseHealthChecker> Logger { get; }

        public string Name { get; }

        public async Task<HealthCheckResult> PerformHealthCheck(CancellationToken cancellationToken = default)
        {
            var healthCheckResult = new HealthCheckResult(Name);

            try
            {
                await PerformHealthCheckImpl(healthCheckResult, cancellationToken);
            }
            catch (Exception e)
            {
                Logger.LogError($"The HealthCheck [{Name}] did not pass", e);
                healthCheckResult.Status = HealthCheckStatus.FAIL;
                healthCheckResult.ErrorMessage = e.Message;
            }

            healthCheckResult.EndTime = DateTime.UtcNow;
            Logger.LogTrace($"Health check {Name} complete. Status {healthCheckResult.Status}");
            return healthCheckResult;
        }

        protected abstract Task PerformHealthCheckImpl(HealthCheckResult healthCheckResult, CancellationToken cancellationToken);
    }
}
