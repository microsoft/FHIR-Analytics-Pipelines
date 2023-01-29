// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class HealthStatusMetric : DiagnosticMetrics
    {
        public HealthStatusMetric(string component, bool isDiagnostic)
            : base(MetricNames.HealthStatusMetric, MetricsCategories.Health, new Dictionary<string, object>
            {
                { DimensionNames.Name, MetricNames.HealthStatusMetric },
                { DimensionNames.Component, component },
                { DimensionNames.Category, MetricsCategories.Health },
                { DimensionNames.IsDiagnostic, isDiagnostic },
                { DimensionNames.Operation, Operations.HealthCheck },
            })
        {
        }
    }
}
