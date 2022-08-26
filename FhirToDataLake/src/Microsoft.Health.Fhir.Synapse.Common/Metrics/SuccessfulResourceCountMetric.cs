// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class SuccessfulResourceCountMetric : ExternalMetrics
    {
        public SuccessfulResourceCountMetric()
            : base("SuccessfulResourceCount", MetricsCategories.Availability, new Dictionary<string, object>
            {
                { DimensionNames.Name, "SuccessfulResourceCount" },
                { DimensionNames.Category, MetricsCategories.Availability },
                { DimensionNames.IsInternal, false },
            })
        {
        }
    }
}
