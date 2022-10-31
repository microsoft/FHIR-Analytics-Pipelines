// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public abstract class BaseHealthChecker : IHealthChecker
    {
        private readonly IDiagnosticLogger _diagnosticLogger;

        protected BaseHealthChecker(
            string healthCheckName,
            bool isCritical,
            IDiagnosticLogger diagnosticLogger,
            ILogger<BaseHealthChecker> logger)
        {
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            Logger = EnsureArg.IsNotNull(logger, nameof(logger));
            Name = EnsureArg.IsNotNullOrWhiteSpace(healthCheckName, nameof(healthCheckName));
            IsCritical = isCritical;
        }

        protected ILogger<BaseHealthChecker> Logger { get; }

        public string Name { get; set; }

        public bool IsCritical { get; set; } = true;

        public async Task<HealthCheckResult> PerformHealthCheckAsync(CancellationToken cancellationToken = default)
        {
            HealthCheckResult healthCheckResult;
            try
            {
                healthCheckResult = await PerformHealthCheckImplAsync(cancellationToken);
            }
            catch (Exception e)
            {
                healthCheckResult = new HealthCheckResult(Name, IsCritical)
                {
                    Status = HealthCheckStatus.UNHEALTHY,
                    ErrorMessage = $"Unhandled exception : {e.Message}.",
                };

                _diagnosticLogger.LogError($"Unknown exception occured in health check component {Name}. {e.Message}");
                Logger.LogInformation(e, $"Unhandled exception occured in health check component {Name}. {e.Message}");
            }

            if (healthCheckResult.Status is HealthCheckStatus.UNHEALTHY)
            {
                _diagnosticLogger.LogWarning($"Health check component {Name} is unhealthy. Failed reason: {healthCheckResult.ErrorMessage}");
                Logger.LogInformation($"Health check component {Name} is unhealthy. Failed reason: {healthCheckResult.ErrorMessage}");
            }
            else
            {
                _diagnosticLogger.LogInformation($"Health check component {Name} is healthy");
                Logger.LogInformation($"Health check component {Name} is healthy");
            }

            return healthCheckResult;
        }

        protected abstract Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken);
    }
}
