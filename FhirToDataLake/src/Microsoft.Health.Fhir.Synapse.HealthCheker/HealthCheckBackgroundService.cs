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
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck
{
    public class HealthCheckBackgroundService : BackgroundService
    {
        private readonly IHealthCheckEngine _healthCheckEngine;
        private readonly IEnumerable<IHealthCheckListener> _healthCheckListeners;
        private readonly ILogger<HealthCheckBackgroundService> _logger;
        private TimeSpan _checkIntervalInSeconds;

        public HealthCheckBackgroundService(
            IHealthCheckEngine healthCheckEngine,
            IEnumerable<IHealthCheckListener> healthCheckListeners,
            IOptions<HealthCheckConfiguration> healthCheckConfiguration,
            ILogger<HealthCheckBackgroundService> logger)
        {
            _healthCheckEngine = EnsureArg.IsNotNull(healthCheckEngine, nameof(healthCheckEngine));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
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

                    // Delay interval time.
                    var delayTask = Task.Delay(_checkIntervalInSeconds, cancellationToken);

                    // Perform health check.
                    var healthStatus = new HealthStatus();
                    await _healthCheckEngine.CheckHealthAsync(healthStatus, cancellationToken);
                    var listenerTasks = _healthCheckListeners.Select(l => l.ProcessHealthStatusAsync(healthStatus, cancellationToken)).ToList();
                    await Task.WhenAll(listenerTasks);
                    healthStatus.EndTime = DateTimeOffset.UtcNow;
                    await delayTask;
                }
                catch (Exception e)
                {
                    _logger.LogError(e, e.ToString());
                    throw;
                }
            }

            _logger.LogInformation("Health check service stopped.");
        }
    }
}
