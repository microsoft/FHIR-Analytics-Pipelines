// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Jobs
{
    /// <summary>
    /// The filter information of a job, which is immutable once a job is created.
    /// </summary>
    public class FilterInfo
    {
        public FilterInfo(
            FilterScope filterScope,
            string groupId,
            DateTimeOffset since,
            IEnumerable<TypeFilter> typeFilters,
            Dictionary<string, int> processedPatients)
        {
            FilterScope = filterScope;
            GroupId = groupId;
            Since = since;
            TypeFilters = typeFilters ?? new List<TypeFilter>();
            ProcessedPatients = processedPatients ?? new Dictionary<string, int>();
        }

        /// <summary>
        /// The filter scope
        /// </summary>
        [JsonProperty("filterScope")]
        public FilterScope FilterScope { get; }

        /// <summary>
        /// The group id
        /// </summary>
        [JsonProperty("groupId")]
        public string GroupId { get; }

        /// <summary>
        /// The start timestamp specified in job configuration.
        /// </summary>
        [JsonProperty("since")]
        public DateTimeOffset Since { get; }

        /// <summary>
        /// The type filters
        /// </summary>
        [JsonProperty("typeFilters")]
        public IEnumerable<TypeFilter> TypeFilters { get; }

        /// <summary>
        /// The patient ids have been processed in the previous jobs and their version ids.
        /// For these patients, we only retrieve the patient resources when they are updated.
        /// </summary>
        [JsonProperty("processedPatients")]
        public Dictionary<string, int> ProcessedPatients { get; }
    }
}
