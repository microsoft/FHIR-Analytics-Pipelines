// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public interface ITypeFilterParser
    {
        /// <summary>
        /// Create a list of <see cref="TypeFilter"/> objects from input string.
        /// Will validate:
        /// 1. the types are valid resource types
        /// 2. for group export, the types are patient compartment resource types
        /// 3. the resource type in typeFilter is in types
        /// 4. the parameters are supported parameters, search result parameters aren't supported
        /// </summary>
        /// <param name="filterScope">the filter scope.</param>
        /// <param name="typeString">the input typeString.</param>
        /// <param name="filterString">the input filterString.</param>
        /// <returns>a list of <see cref="TypeFilter"/> objects, will throw an exception if invalid.</returns>
        public IEnumerable<TypeFilter> CreateTypeFilters(
            FilterScope filterScope,
            string typeString,
            string filterString);
    }
}
