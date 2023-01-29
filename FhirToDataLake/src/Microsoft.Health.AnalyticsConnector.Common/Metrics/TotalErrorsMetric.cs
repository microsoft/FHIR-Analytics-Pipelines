// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.AnalyticsConnector.Common.Metrics
{
    public class TotalErrorsMetric : DiagnosticMetrics
    {
        public TotalErrorsMetric(bool isDiagnostic, string errorType, string reason, string operation)
            : base(MetricNames.TotalErrorsMetric, MetricsCategories.Errors, new Dictionary<string, object>
            {
                { DimensionNames.Name, MetricNames.TotalErrorsMetric },
                { DimensionNames.Category, MetricsCategories.Errors },
                { DimensionNames.IsDiagnostic, isDiagnostic },
                { DimensionNames.ErrorType, errorType },
                { DimensionNames.Reason, reason },
                { DimensionNames.Operation, operation },
            })
        {
        }
    }
}