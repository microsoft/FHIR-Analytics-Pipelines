// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
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
            HealthCheckOptions healthCheckOptions,
            ILogger<HealthCheckEngine> logger)
        {
            EnsureArg.IsNotNull(healthCheckOptions, nameof(healthCheckOptions));
            _healthCheckers = EnsureArg.IsNotNull(healthCheckers, nameof(healthCheckers));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _healthCheckTimeout = healthCheckOptions.HealthCheckTimeout;
        }

        public async Task CheckHealthAsync(HealthStatus healthStatus, AsyncCallback callBack = null, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(healthStatus, nameof(healthStatus));

            var tasks = new Task[_healthCheckers.Count()];
            var collectedResults = new ConcurrentDictionary<string, HealthCheckResult>();
            var taskNumber = 0;

            // healthCheckToken will be canceled if health check timeout or cancellationToken is canceled.
            using var healthCheckToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            healthCheckToken.CancelAfter(_healthCheckTimeout);

            InitializeHealthCheckResults(_healthCheckers, collectedResults);

            foreach (var healthCheck in _healthCheckers)
            {
                tasks[taskNumber++] = ExecuteHealthCheck(healthCheck, collectedResults, healthCheckToken.Token);
            }

            await Task.WhenAll(tasks);

            healthStatus.EndTime = DateTime.UtcNow;
            healthStatus.HealthCheckResults = collectedResults.Values.ToList();

            _logger.LogTrace($"Finished health checks: ${string.Join(',', healthStatus.HealthCheckResults.Select(x => x.Name))}. Time using : {(healthStatus.EndTime - healthStatus.StartTime).TotalSeconds} seconds.");
        }

        private static void InitializeHealthCheckResults(IEnumerable<IHealthChecker> healthCheckers, ConcurrentDictionary<string, HealthCheckResult> results)
        {
            foreach (var healthChecker in healthCheckers)
            {
                results[healthChecker.Name] = new HealthCheckResult(healthChecker.Name);
            }
        }

        private static async Task ExecuteHealthCheck(IHealthChecker healthChecker, ConcurrentDictionary<string, HealthCheckResult> results, CancellationToken cancellationToken)
        {
            // Perform the actual health check and override the initial result
            results[healthChecker.Name] = await healthChecker.PerformHealthCheck(cancellationToken);
        }
    }
}
