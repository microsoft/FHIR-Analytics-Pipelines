using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public interface ITypeFilterParser
    {
        /// <summary>
        /// Parse type filter and validate it.
        /// 1. the types are valid resource types
        /// 2. for group export, the types are patient compartment resource types
        /// 3. the resource type in typefilter is in types
        /// 4. the parameters are supported parameters, search result parameters aren't supported
        /// </summary>
        /// <param name="filterScope">the filter scope.</param>
        /// <param name="typeString">the input typeString.</param>
        /// <param name="filterString">the input filterString.</param>
        /// <returns>type filter list</returns>
        public IEnumerable<TypeFilter> CreateTypeFilters(
            FilterScope filterScope,
            string typeString,
            string filterString);
    }
}
