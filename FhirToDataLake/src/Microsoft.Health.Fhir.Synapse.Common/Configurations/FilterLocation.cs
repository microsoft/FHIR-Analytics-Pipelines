// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class FilterLocation
    {
        /// <summary>
        /// Gets or sets bool value for whether enable external filter configuration from acr.
        /// </summary>
        [JsonProperty("enableExternalFilter")]
        public bool EnableExternalFilter { get; set; } = false;

        /// <summary>
        /// Azure Container Registry image reference of filter configuration file.
        /// </summary>
        [JsonProperty("filterImageReference")]
        public string FilterImageReference { get; set; } = string.Empty;
    }
}
