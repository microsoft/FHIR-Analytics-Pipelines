// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Models
{
    public class OverallHealthStatus
    {
        public DateTimeOffset StartTime { get; set; } = DateTimeOffset.UtcNow;

        public IList<HealthCheckResult> HealthCheckResults { get; set; }

        public HealthCheckStatus Status
        {
            get
            {
                return HealthCheckResults.Select(x => x.Status == HealthCheckStatus.UNHEALTHY && x.IsCritical == true).Any() ? HealthCheckStatus.UNHEALTHY : HealthCheckStatus.HEALTHY;
            }
        }
    }
}
