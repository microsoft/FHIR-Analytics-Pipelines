// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class ProcessLatencyMetric : DiagnosticMetrics
    {
        public ProcessLatencyMetric()
            : base("ResourceLatency", MetricsCategories.Latency, new Dictionary<string, object>
            {
                { DimensionNames.Name, "ResourceLatency" },
                { DimensionNames.Category, MetricsCategories.Latency },
                { DimensionNames.IsDiagnostic, true },
                { DimensionNames.Operation, Operations.CompleteJob },
            })
        {
        }
    }
}
