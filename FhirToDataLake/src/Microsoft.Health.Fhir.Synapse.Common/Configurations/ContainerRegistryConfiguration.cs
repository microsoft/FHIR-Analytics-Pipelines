// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class ContainerRegistryConfiguration
    {
        /// <summary>
        /// Gets Azure Container Registry image reference of customized schema templates.
        /// </summary>
        [JsonProperty("schemaImageReference")]
        public string SchemaImageReference { get; set; } = string.Empty;
    }
}
