// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests
{
    public class MockTimeoutHealthChecker : BaseHealthChecker
    {
        public MockTimeoutHealthChecker(ILogger<MockTimeoutHealthChecker> logger)
                : base("MockTimeout", true, logger)
        {
        }

        protected override async Task PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(300), cancellationToken);
        }
    }
}
