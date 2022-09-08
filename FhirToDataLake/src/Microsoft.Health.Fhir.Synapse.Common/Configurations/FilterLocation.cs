// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class FilterLocation
    {
        /// <summary>
        /// The filter scope, "System" and "Group" are supported now.
        /// </summary>
        [JsonProperty("enableExternalFilter")]
        public bool EnableExternalFilter { get; set; } = false;

        /// <summary>
        /// The group id for "Group" filter scope.
        /// </summary>
        [JsonProperty("filterImageReference")]
        public string FilterImageReference { get; set; } = string.Empty;

        /// <summary>
        /// The group id for "Group" filter scope.
        /// </summary>
        [JsonProperty("filterConfigurationFileName")]
        public string FilterConfigurationFileName { get; set; } = "filterConfiguration.json";
    }
}
