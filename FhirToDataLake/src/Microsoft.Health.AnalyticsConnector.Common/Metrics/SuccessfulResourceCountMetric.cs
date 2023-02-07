﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.AnalyticsConnector.Common.Metrics
{
    public class SuccessfulResourceCountMetric : DiagnosticMetrics
    {
        public SuccessfulResourceCountMetric(string operation = null)
            : base(MetricNames.SuccessfulResourceCountMetric, MetricsCategories.Availability, new Dictionary<string, object>
            {
                { DimensionNames.Name, MetricNames.SuccessfulResourceCountMetric },
                { DimensionNames.Category, MetricsCategories.Availability },
                { DimensionNames.IsDiagnostic, true },
                { DimensionNames.Operation, JobOperations.CompleteJob },
            })
        {
        }
    }
}
