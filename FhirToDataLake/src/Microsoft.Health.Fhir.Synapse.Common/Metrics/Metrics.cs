// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public abstract class Metrics
    {
        public Metrics(string name, string category, IDictionary<string, object> dimensions)
        {
            Name = name;
            Category = category;
            Dimensions = dimensions;
        }

        public string Name { get; set; }

        public string Category { get; set; }

        public IDictionary<string, object> Dimensions { get; }
    }
}
