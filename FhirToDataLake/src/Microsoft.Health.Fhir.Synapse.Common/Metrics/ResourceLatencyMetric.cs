// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class ResourceLatencyMetric : DiagnosticMetrics
    {
        public ResourceLatencyMetric()
            : base(MetricNames.ResourceLatencyMetric, MetricsCategories.Latency, new Dictionary<string, object>
            {
                { DimensionNames.Name, MetricNames.ResourceLatencyMetric },
                { DimensionNames.Category, MetricsCategories.Latency },
                { DimensionNames.IsDiagnostic, true },
                { DimensionNames.Operation, JobOperations.CompleteJob },
            })
        {
        }
    }
}
