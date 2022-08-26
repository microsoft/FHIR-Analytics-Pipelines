// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public abstract class InternalMetrics : Metrics
    {
        protected InternalMetrics(string name, MetricsCategories category, IDictionary<string, object> dimensions)
            : base(name, category, dimensions)
        {
        }
    }
}
