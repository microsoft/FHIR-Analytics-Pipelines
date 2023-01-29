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
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck
{
    public class HealthCheckBackgroundService : BackgroundService
    {
        private readonly IHealthCheckEngine _healthCheckEngine;
        private readonly IEnumerable<IHealthCheckListener> _healthCheckListeners;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<HealthCheckBackgroundService> _logger;
        private readonly IMetricsLogger _metricsLogger;
        private TimeSpan _checkIntervalInSeconds;

        public HealthCheckBackgroundService(
            IHealthCheckEngine healthCheckEngine,
            IEnumerable<IHealthCheckListener> healthCheckListeners,
            IOptions<HealthCheckConfiguration> healthCheckConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<HealthCheckBackgroundService> logger,
            IMetricsLogger metricsLogger)
        {
            _healthCheckEngine = EnsureArg.IsNotNull(healthCheckEngine, nameof(healthCheckEngine));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
            _healthCheckListeners = EnsureArg.IsNotNull(healthCheckListeners, nameof(healthCheckListeners));
            EnsureArg.IsNotNull(healthCheckConfiguration, nameof(healthCheckConfiguration));

            _checkIntervalInSeconds = TimeSpan.FromSeconds(healthCheckConfiguration.Value.HealthCheckTimeIntervalInSeconds);
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Delay interval time.
                Task delayTask = Task.Delay(_checkIntervalInSeconds, cancellationToken);
                try
                {
                    _diagnosticLogger.LogInformation("Starting to perform health checks");
                    _logger.LogInformation("Starting to perform health checks");

                    // Perform health check.
                    OverallHealthStatus healthStatus = await _healthCheckEngine.CheckHealthAsync(cancellationToken);

                    _diagnosticLogger.LogInformation($"Finished health checks: {string.Join(',', healthStatus.HealthCheckResults.Select(x => string.Format("{0}:{1}", x.Name, x.Status)))}.");
                    _logger.LogInformation($"Finished health checks: {string.Join(',', healthStatus.HealthCheckResults.Select(x => string.Format("{0}:{1}", x.Name, x.Status)))}.");

                    // Todo: Send notification to mediator and remove listeners here.
                    List<Task> listenerTasks = _healthCheckListeners.Select(l => l.ProcessHealthStatusAsync(healthStatus, cancellationToken)).ToList();
                    foreach (HealthCheckResult component in healthStatus.HealthCheckResults)
                    {
                        _metricsLogger.LogHealthStatusMetric(component.Name, !component.IsCritical, (double)component.Status);
                        _logger.LogInformation($"Report a {MetricNames.HealthStatusMetric} metric for component {component.Name} with status {component.Status}");
                    }

                    await Task.WhenAll(listenerTasks);
                    await delayTask;
                }
                catch (OperationCanceledException e)
                {
                    // not expected to trigger this exception.
                    _diagnosticLogger.LogError($"Health check service is cancelled. Reason: {e.Message}");
                    _logger.LogInformation(e, $"Health check service is cancelled. Reason: {e.Message}");
                    _metricsLogger.LogTotalErrorsMetrics(e, $"Health check service is cancelled. Reason: {e.Message}", JobOperations.HealthCheck);
                    await delayTask;
                    throw;
                }
                catch (Exception e)
                {
                    _diagnosticLogger.LogError($"Unknown exception occured in health check. {e.Message}");
                    _logger.LogError(e, $"Unhandled exception occured in health check. {e.Message}");
                    _metricsLogger.LogTotalErrorsMetrics(e, $"Unhandled exception occured in health check. {e.Message}", JobOperations.HealthCheck);
                    await delayTask;
                }
            }

            _diagnosticLogger.LogInformation("Health check service stopped.");
            _logger.LogInformation("Health check service stopped.");
        }
    }
}
