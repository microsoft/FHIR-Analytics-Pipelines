// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class TotalErrorsMetric : DiagnosticMetrics
    {
        public TotalErrorsMetric(string errorType, string reason, string operation)
            : base("TotalError", MetricsCategories.Errors, new Dictionary<string, object>
            {
                { DimensionNames.Name, "TotalError" },
                { DimensionNames.Category, MetricsCategories.Errors },
                { DimensionNames.IsDiagnostic, true },
                { DimensionNames.ErrorType, errorType },
                { DimensionNames.Reason, reason },
                { DimensionNames.Operation, operation },
            })
        {
        }
    }
}