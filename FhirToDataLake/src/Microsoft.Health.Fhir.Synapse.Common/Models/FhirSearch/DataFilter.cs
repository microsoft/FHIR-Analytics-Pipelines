// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch
{
    public class DataFilter
    {
        public DataFilter(IList<string> resourceTypes, IList<TypeFilter> typeFilters)
        {
            ResourceTypes = resourceTypes;
            TypeFilters = typeFilters;
        }

        public IList<string> ResourceTypes { get; set; }

        public IList<TypeFilter> TypeFilters { get; set; }
    }
}
