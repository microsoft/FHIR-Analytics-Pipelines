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
        private readonly TimeSpan _healthCheckTimeout;
        private readonly TimeSpan _healthStatusCollectionTimeout;
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
            _healthStatusCollectionTimeout = healthCheckOptions.HealthStatusCollectionTimeout;
        }

        public async Task CheckHealthAsync(HealthStatus healthStatus, AsyncCallback callBack = null, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(healthStatus, nameof(healthStatus));

            healthStatus.StartTime = DateTime.UtcNow;
            var tasks = new Task[_healthCheckers.Count()];
            var collectedResults = new ConcurrentDictionary<string, HealthCheckResult>();
            var taskNumber = 0;

            using var overallHealthStatusToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            using var healthCheckToken = CancellationTokenSource.CreateLinkedTokenSource(overallHealthStatusToken.Token);
            overallHealthStatusToken.CancelAfter(_healthStatusCollectionTimeout);
            healthCheckToken.CancelAfter(_healthCheckTimeout);

            InitializeHealthCheckResults(_healthCheckers, collectedResults);

            foreach (var healthCheck in _healthCheckers)
            {
                tasks[taskNumber++] = ExecuteHealthCheck(healthCheck, collectedResults, healthCheckToken.Token);
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (OperationCanceledException)
            {
                var now = DateTime.UtcNow;
                var names = new List<string>();

                foreach (var timedOutHealthCheck in collectedResults.Values.Where(check => HealthCheckStatus.UNKNOWN.Equals(check.Status)))
                {
                    timedOutHealthCheck.EndTime = now;
                    timedOutHealthCheck.ErrorMessage = $"HealthChecks exceeded the overall Health Status timeout of ${_healthStatusCollectionTimeout}";
                    names.Add(timedOutHealthCheck.Name);
                }

                _logger.LogTrace($"One or more HealthChecks exceeded the overall Health Status timeout of ${_healthStatusCollectionTimeout}. Failed HealthChecks: ${string.Join(',', names)}");
            }

            healthStatus.EndTime = DateTime.UtcNow;
            healthStatus.HealthCheckResults = collectedResults.Values.ToList();
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
