// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Models
{
    public class HealthStatus
    {
        public DateTimeOffset StartTime { get; set; } = DateTimeOffset.UtcNow;

        public DateTimeOffset EndTime { get; set; } = DateTimeOffset.UtcNow;

        public IList<HealthCheckResult> HealthCheckResults { get; set; }
    }
}
