// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.HealthCheck;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck
{
    public interface IHealthCheckEngine
    {
        Task<OverallHealthStatus> CheckHealthAsync(CancellationToken cancellationToken = default);
    }
}
