// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck
{
    public class HealthCheckEngine : IHealthCheckEngine
    {
        private readonly IEnumerable<IHealthChecker> _healthCheckers;

        // Timeout for each health checkers.
        private readonly TimeSpan _healthCheckTimeoutInSeconds;

        public HealthCheckEngine(
            IEnumerable<IHealthChecker> healthCheckers,
            IOptions<HealthCheckConfiguration> healthCheckConfiguration)
        {
            EnsureArg.IsNotNull(healthCheckConfiguration, nameof(healthCheckConfiguration));
            _healthCheckers = EnsureArg.IsNotNull(healthCheckers, nameof(healthCheckers));

            _healthCheckTimeoutInSeconds = TimeSpan.FromSeconds(healthCheckConfiguration.Value.HealthCheckTimeoutInSeconds);
        }

        public async Task<OverallHealthStatus> CheckHealthAsync(CancellationToken cancellationToken = default)
        {
            var healthStatus = new OverallHealthStatus();
            var tasks = new List<Task<HealthCheckResult>>();

            // healthCheckToken will be canceled if health check timeout or cancellationToken is canceled.
            using var healthCheckToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            healthCheckToken.CancelAfter(_healthCheckTimeoutInSeconds);

            foreach (var healthChecker in _healthCheckers)
            {
                tasks.Add(healthChecker.PerformHealthCheckAsync(healthCheckToken.Token));
            }

            healthStatus.HealthCheckResults = await Task.WhenAll(tasks);

            return healthStatus;
        }
    }
}
