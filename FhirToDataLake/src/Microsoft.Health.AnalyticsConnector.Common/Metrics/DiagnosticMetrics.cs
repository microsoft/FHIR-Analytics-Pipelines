// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.AnalyticsConnector.Common.Metrics
{
    public class DiagnosticMetrics : Metrics
    {
        protected DiagnosticMetrics(string name, MetricsCategories category, IDictionary<string, object> dimensions)
            : base(name, category, dimensions)
        {
        }
    }
}
