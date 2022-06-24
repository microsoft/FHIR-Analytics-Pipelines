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
        /// The job scope, "System" and "Group" are supported now.
        /// </summary>
        [JsonProperty("jobScope")] public JobScope JobScope { get; set; } = JobScope.System;

        /// <summary>
        /// The group id for "Group" job scope.
        /// </summary>
        [JsonProperty("groupId")] public string GroupId { get; set; } = string.Empty;

        /// <summary>
        /// Selected resource types, delimited by comma.
        /// If no resources are present, we will process all resource types.
        /// </summary>
        [JsonProperty("type")]
        public string RequiredTypes { get; set; } = string.Empty;

        /// <summary>
        /// type filter string, delimited by comma
        /// </summary>
        [JsonProperty("typeFilter")]
        public string TypeFilters { get; set; } = string.Empty;
    }
}