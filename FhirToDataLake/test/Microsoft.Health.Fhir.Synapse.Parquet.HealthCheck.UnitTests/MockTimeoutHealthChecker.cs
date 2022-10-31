// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests
{
    public class MockTimeoutHealthChecker : BaseHealthChecker
    {
        public MockTimeoutHealthChecker(IDiagnosticLogger diagnosticLogger, ILogger<MockTimeoutHealthChecker> logger)
                : base("MockTimeout", true, diagnosticLogger, logger)
        {
        }

        protected override async Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            var result = new HealthCheckResult("MockTimeout", true);
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(1500), cancellationToken);
                result.Status = HealthCheckStatus.HEALTHY;
            }
            catch
            {
                result.Status = HealthCheckStatus.UNHEALTHY;
            }

            return result;
        }
    }
}
