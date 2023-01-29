// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class SuccessfulDataSizeMetric : DiagnosticMetrics
    {
        public SuccessfulDataSizeMetric()
            : base(MetricNames.SuccessfulDataSizeMetric, MetricsCategories.Availability, new Dictionary<string, object>
            {
                { DimensionNames.Name, MetricNames.SuccessfulDataSizeMetric },
                { DimensionNames.Category, MetricsCategories.Availability },
                { DimensionNames.IsDiagnostic, true },
                { DimensionNames.Operation, Operations.CompleteJob },
            })
        {
        }
    }
}
