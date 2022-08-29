// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class HealthStatusMetric : DiagnosticMetrics
    {
        public HealthStatusMetric()
            : base("HealthStatus", MetricsCategories.Health, new Dictionary<string, object>
            {
                { DimensionNames.Name, "HealthStatus" },
                { DimensionNames.Category, MetricsCategories.Health },
                { DimensionNames.IsDiagnostic, false },
                { DimensionNames.Operation, Operations.HealthCheck },
            })
        {
        }
    }
}
