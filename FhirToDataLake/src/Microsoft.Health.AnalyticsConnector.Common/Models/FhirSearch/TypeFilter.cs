// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using EnsureThat;
using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Common.Models.FhirSearch
{
    public class TypeFilter
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TypeFilter"/> class.
        /// We will trigger a http request for each typeFilter,
        /// </summary>
        /// <param name="resourceType">resource type</param>
        /// <param name="parameters">query parameters</param>
        public TypeFilter(string resourceType, IList<KeyValuePair<string, string>> parameters)
        {
            ResourceType = EnsureArg.IsNotEmptyOrWhiteSpace(resourceType, nameof(resourceType));
            Parameters = parameters ?? new List<KeyValuePair<string, string>>();
        }

        /// <summary>
        /// Resource type, if the resourceType is "*" means want to get all the compartment resources.
        /// </summary>
        [JsonProperty("resourceType")]
        public string ResourceType { get; set; }

        /// <summary>
        /// The parameters, which is a parameter name/value pair list.
        /// We should use List instead of dictionary here, since the parameter keys may be the same, such as lastUpdated=gt1900-01-01&lastUpdated=lt2000-01-01
        /// </summary>
        [JsonProperty("parameters")]
        public IList<KeyValuePair<string, string>> Parameters { get; set; }
    }
}
