// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch
{
    public class TypeFilter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TypeFilter"/> class.
        /// We will trigger a http request for each typeFilter,
        /// if the resourceType is "*" means want to get all the compartment resources.
        /// </summary>
        /// <param name="resourceType">resource type</param>
        /// <param name="parameters">query parameters</param>
        public TypeFilter(string resourceType, IList<Tuple<string, string>> parameters)
        {
            ResourceType = resourceType;
            Parameters = parameters ?? new List<Tuple<string, string>>();
        }

        [JsonProperty("resourceType")]
        public string ResourceType { get; set; }

        // we should use List here, the parameter keys may be the same, such as lastUpdated=gt1900-01-01&lastUpdated=lt2000-01-01
        [JsonProperty("parameters")]
        public IList<Tuple<string, string>> Parameters { get; set; }
    }
}
