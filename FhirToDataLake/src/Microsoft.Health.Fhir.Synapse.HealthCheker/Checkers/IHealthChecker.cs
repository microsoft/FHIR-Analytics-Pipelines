// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Checkers
{
    public interface IHealthChecker
    {
        string Name { get; set; }

        Task<HealthCheckResult> PerformHealthCheckAsync(CancellationToken cancellationToken = default);
    }
}
