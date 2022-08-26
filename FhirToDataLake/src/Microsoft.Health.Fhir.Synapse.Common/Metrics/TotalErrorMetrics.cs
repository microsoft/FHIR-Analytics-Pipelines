// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class TotalErrorMetrics : ExternalMetrics
    {
        public TotalErrorMetrics()
            : base("TotalError", MetricsCategories.Errors, new Dictionary<string, object>
            {
                { DimensionNames.Name, "TotalError" },
                { DimensionNames.Category, MetricsCategories.Errors },
                { DimensionNames.IsInternal, false },
            })
        {
        }
    }
}