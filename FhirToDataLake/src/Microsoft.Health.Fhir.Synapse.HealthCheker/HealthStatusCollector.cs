// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Checkers;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker
{
    public class HealthStatusCollector : IHealthStatusCollector
    {
        private readonly IEnumerable<IHealthChecker> _healthCheckers;
        private readonly TimeSpan _healthCheckTimeout;
        private readonly TimeSpan _healthStatusCollectionInterval;
        ILogger<HealthStatusCollector> _logger;

        public HealthStatusCollector(
            IEnumerable<IHealthChecker> healthCheckers,
            HealthCheckOptions healthCheckOptions,
            ILogger<HealthStatusCollector> logger)
        {
            EnsureArg.IsNotNull(healthCheckOptions, nameof(healthCheckOptions));
            _healthCheckers = EnsureArg.IsNotNull(healthCheckers, nameof(healthCheckers));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            _healthCheckTimeout = healthCheckOptions.HealthCheckTimeout;
            _healthStatusCollectionInterval = healthCheckOptions.CheckInterval;
        }

        public async Task<HealthStatus> CheckHealthAsync(CancellationToken cancellationToken = default)
        {
            // Build a Health Status Object.
            var healthStatus = new HealthStatus();
            var tasks = new Task[_healthCheckers.Count()];
            var collectedResults = new ConcurrentDictionary<string, HealthCheckResult>();
            var taskNumber = 0;

            using var overallHealthStatusToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            using var healthCheckToken = CancellationTokenSource.CreateLinkedTokenSource(overallHealthStatusToken.Token);
            overallHealthStatusToken.CancelAfter(_healthStatusCollectionInterval);
            healthCheckToken.CancelAfter(_healthCheckTimeout);

            foreach (var healthCheck in _healthCheckers)
            {
                tasks[taskNumber++] = ExecuteHealthCheck(healthCheck, collectedResults, healthCheckToken.Token);
            }

            try
            {
                Task.WaitAll(tasks, overallHealthStatusToken.Token);
            }
            catch (OperationCanceledException)
            {
                var now = DateTime.UtcNow;
                var names = new List<string>();

                foreach (var timedOutHealthCheck in collectedResults.Values.Where(check => HealthCheckStatus.UNKNOWN.Equals(check.Status)))
                {
                    timedOutHealthCheck.EndTime = now;
                    timedOutHealthCheck.ErrorMessage = $"HealthChecks exceeded the overall Health Status timeout of ${_healthStatusCollectionInterval}";
                    names.Add(timedOutHealthCheck.Name);
                }

                _logger.LogTrace($"One or more HealthChecks exceeded the overall Health Status timeout of ${_healthStatusCollectionInterval}. Failed HealthChecks: ${string.Join(',', names)}");
            }

            healthStatus.EndTime = DateTime.UtcNow;
            healthStatus.HealthCheckResults = collectedResults.Values.ToList();

            return healthStatus;
        }

        private static async Task ExecuteHealthCheck(IHealthChecker healthChecker, ConcurrentDictionary<string, HealthCheckResult> results, CancellationToken cancellationToken)
        {
            /*
             * Creates an initial HealthCheckResult for this HealthCheck which will be in the UNKNOWN state. If PerformHealthCheck does not return
             * (i.e. the operation hangs) then a status UNKNOWN will be reported for the HealthCheck
             */
            var initialResult = new HealthCheckResult(healthChecker.Name);

            results[healthChecker.Name] = initialResult;

            // Perform the actual health check and override the initial result
            results[healthChecker.Name] = await healthChecker.PerformHealthCheck(cancellationToken);
        }
    }
}
