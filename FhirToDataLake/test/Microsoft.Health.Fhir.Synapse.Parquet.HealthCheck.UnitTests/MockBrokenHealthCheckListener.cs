﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests
{
    public class MockBrokenHealthCheckListener : IHealthCheckListener
    {
        public Task ProcessHealthStatusAsync(OverallHealthStatus healthStatus, CancellationToken cancellationToken)
        {
            throw new Exception();
        }
    }
}
