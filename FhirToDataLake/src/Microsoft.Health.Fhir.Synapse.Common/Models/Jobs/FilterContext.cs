// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Jobs
{
    public class FilterContext
    {
        public FilterContext(
            FilterScope filterScope,
            string groupId,
            IEnumerable<TypeFilter> typeFilters,
            IEnumerable<string> processedPatientIds)
        {
            FilterScope = filterScope;
            GroupId = groupId;
            TypeFilters = typeFilters;
            ProcessedPatientIds = processedPatientIds ?? new HashSet<string>();
        }

        [JsonProperty("filterScope")]
        public FilterScope FilterScope { get; }

        [JsonProperty("groupId")]
        public string GroupId { get; }

        [JsonProperty("typeFilters")]
        public IEnumerable<TypeFilter> TypeFilters { get; }

        [JsonProperty("processedPatientIds")]
        public IEnumerable<string> ProcessedPatientIds { get; }

    }
}
