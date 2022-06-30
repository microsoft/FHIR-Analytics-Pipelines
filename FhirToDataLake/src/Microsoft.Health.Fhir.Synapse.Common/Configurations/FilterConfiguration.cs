// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class FilterConfiguration
    {
        /// <summary>
        /// The filter scope, "System" and "Group" are supported now.
        /// </summary>
        [JsonProperty("filterScope")]
        public FilterScope FilterScope { get; set; } = FilterScope.System;

        /// <summary>
        /// The group id for "Group" filter scope.
        /// </summary>
        [JsonProperty("groupId")]
        public string GroupId { get; set; } = string.Empty;

        /// <summary>
        /// Selected resource types, delimited by comma.
        /// If no resources are present, we will process all resource types.
        /// </summary>
        [JsonProperty("requiredTypes")]
        public string RequiredTypes { get; set; } = string.Empty;

        /// <summary>
        /// type filter string, delimited by comma
        /// </summary>
        [JsonProperty("typeFilters")]
        public string TypeFilters { get; set; } = string.Empty;
    }
}