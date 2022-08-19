// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using MediatR;
using Microsoft.Health.Fhir.Synapse.Common.Models.HealthCheck;

namespace Microsoft.Health.Fhir.Synapse.Common.Notification
{
    public class HealthCheckNotification : INotification
    {
        public HealthCheckNotification(OverallHealthStatus status)
        {
            StartTime = status.StartTime;
            HealthCheckResults = status.HealthCheckResults;
        }

        public DateTimeOffset StartTime { get; set; } = DateTimeOffset.UtcNow;

        public IList<HealthCheckResult> HealthCheckResults { get; set; }

        public HealthCheckStatus Status
        {
            get
            {
                return HealthCheckResults.Any(x => x.Status == HealthCheckStatus.UNHEALTHY && x.IsCritical == true) ? HealthCheckStatus.UNHEALTHY : HealthCheckStatus.HEALTHY;
            }
        }
    }
}
