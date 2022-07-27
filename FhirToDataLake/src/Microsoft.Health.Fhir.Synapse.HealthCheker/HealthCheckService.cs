// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker
{
    public class HealthCheckService : IHealthCheckService
    {
        private static IHealthCheckEngine _healthCheckEngine;
        private readonly ILogger<HealthCheckService> _logger;
        private HealthStatus _healthStatus;
        private AsyncCallback _callBack;

        public HealthCheckService(
            IHealthCheckEngine healthCheckEngine,
            ILogger<HealthCheckService> logger)
        {
            _healthCheckEngine = EnsureArg.IsNotNull(healthCheckEngine, nameof(healthCheckEngine));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _callBack = new (HealthCheckCallbackResultProcess);
        }

        public void Execute(CancellationToken cancellationToken)
        {
            _healthStatus = new ();
            try
            {
                _healthCheckEngine.CheckHealthAsync(_healthStatus, _callBack, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Health check failed : {ex}");
            }
        }

        public void HealthCheckCallbackResultProcess(IAsyncResult result)
        {
            foreach (var oneResult in _healthStatus.HealthCheckResults)
            {
                _logger.LogTrace($"{oneResult.Name} : {oneResult.Status}");
            }

            _logger.LogTrace("Finished");
        }
    }
}
