// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Checkers;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker
{
    public class HealthCheckEngine : IHealthCheckEngine
    {
        private readonly IEnumerable<IHealthChecker> _healthCheckers;

        // Timeout for each health checkers.
        private readonly TimeSpan _healthCheckTimeout;
        private readonly ILogger<HealthCheckEngine> _logger;

        public HealthCheckEngine(
            IEnumerable<IHealthChecker> healthCheckers,
            IOptions<HealthCheckConfiguration> healthCheckConfiguration,
            ILogger<HealthCheckEngine> logger)
        {
            EnsureArg.IsNotNull(healthCheckConfiguration, nameof(healthCheckConfiguration));
            _healthCheckers = EnsureArg.IsNotNull(healthCheckers, nameof(healthCheckers));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _healthCheckTimeout = TimeSpan.FromSeconds(healthCheckConfiguration.Value.HealthCheckTimeoutInSeconds);
        }

        public async Task CheckHealthAsync(HealthStatus healthStatus, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(healthStatus, nameof(healthStatus));

            var tasks = new List<Task>();
            var collectedResults = new Dictionary<string, HealthCheckResult>();

            // healthCheckToken will be canceled if health check timeout or cancellationToken is canceled.
            using var healthCheckToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            healthCheckToken.CancelAfter(_healthCheckTimeout);

            foreach (var healthChecker in _healthCheckers)
            {
                collectedResults[healthChecker.Name] = new HealthCheckResult(healthChecker.Name);
            }

            foreach (var healthChecker in _healthCheckers)
            {
                tasks.Add(ExecuteHealthCheck(healthChecker, collectedResults, healthCheckToken.Token));
            }

            await Task.WhenAll(tasks);

            healthStatus.EndTime = DateTime.UtcNow;
            healthStatus.HealthCheckResults = collectedResults.Values.ToList();

            _logger.LogInformation($"Finished health checks: ${string.Join(',', healthStatus.HealthCheckResults.Select(x => x.Name))}. Time using : {(healthStatus.EndTime - healthStatus.StartTime).TotalSeconds} seconds.");
        }

        private static async Task ExecuteHealthCheck(IHealthChecker healthChecker, Dictionary<string, HealthCheckResult> results, CancellationToken cancellationToken)
        {
            // Perform the actual health check and override the initial result
            results[healthChecker.Name] = await healthChecker.PerformHealthCheckAsync(cancellationToken);
        }
    }
}
