// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using MediatR;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Notification;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck
{
    public class HealthCheckBackgroundService : BackgroundService
    {
        private readonly IHealthCheckEngine _healthCheckEngine;
        private readonly ILogger<HealthCheckBackgroundService> _logger;
        private readonly IMediator _mediator;
        private TimeSpan _checkIntervalInSeconds;

        public HealthCheckBackgroundService(
            IHealthCheckEngine healthCheckEngine,
            IOptions<HealthCheckConfiguration> healthCheckConfiguration,
            ILogger<HealthCheckBackgroundService> logger,
            IMediator mediator)
        {
            _healthCheckEngine = EnsureArg.IsNotNull(healthCheckEngine, nameof(healthCheckEngine));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            _mediator = EnsureArg.IsNotNull(mediator, nameof(mediator));
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
                    var healthStatus = await _healthCheckEngine.CheckHealthAsync(cancellationToken);

                    await _mediator.Publish(new HealthCheckNotification(healthStatus), cancellationToken);
                    await delayTask;
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
            }

            _logger.LogInformation("Health check service stopped.");
        }
    }
}
