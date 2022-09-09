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
using Microsoft.Health.Fhir.Synapse.Common.Metrics;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck
{
    public class HealthCheckBackgroundService : BackgroundService
    {
        private readonly IHealthCheckEngine _healthCheckEngine;
        private readonly IEnumerable<IHealthCheckListener> _healthCheckListeners;
        private readonly ILogger<HealthCheckBackgroundService> _logger;
        private readonly IMetricsLogger _metricsLogger;
        private TimeSpan _checkIntervalInSeconds;

        public HealthCheckBackgroundService(
            IHealthCheckEngine healthCheckEngine,
            IEnumerable<IHealthCheckListener> healthCheckListeners,
            IOptions<HealthCheckConfiguration> healthCheckConfiguration,
            ILogger<HealthCheckBackgroundService> logger,
            IMetricsLogger metricsLogger)
        {
            _healthCheckEngine = EnsureArg.IsNotNull(healthCheckEngine, nameof(healthCheckEngine));
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
                try
                {
                    _logger.LogInformation("Starting to perform health checks");

                    // Perform health check.
                    var healthStatus = await _healthCheckEngine.CheckHealthAsync(cancellationToken);

                    // Todo: Send notification to mediator and remove listeners here.
                    var listenerTasks = _healthCheckListeners.Select(l => l.ProcessHealthStatusAsync(healthStatus, cancellationToken)).ToList();
                    _metricsLogger.LogHealthStatusMetric(healthStatus.Status == Models.HealthCheckStatus.HEALTHY ? 1 : 0);
                    await Task.WhenAll(listenerTasks);
                }
                catch (OperationCanceledException e)
                {
                    // not expected to trigger this exception.
                    _logger.LogError(e, $"Health check service is cancelled. {e.Message}");
                    throw;
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Unhandled exception occured. {e.Message}");
                }

                try
                {
                    // Delay interval time.
                    var delayTask = Task.Delay(_checkIntervalInSeconds, cancellationToken);
                    await delayTask;
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"Health check service is cancelled. {e.Message}");
                    throw;
                }
            }

            _logger.LogInformation("Health check service stopped.");
        }
    }
}
