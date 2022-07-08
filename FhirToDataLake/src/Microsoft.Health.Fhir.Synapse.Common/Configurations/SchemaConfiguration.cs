// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class SchemaConfiguration
    {
        /// <summary>
        /// Gets or sets path of directory that contains schema Json files.
        /// For now we only support loading schemas from default directory.
        /// </summary>
        [JsonProperty("schemaCollectionDirectory")]
        public string SchemaCollectionDirectory { get; set; } = ConfigurationConstants.DefaultSchemaDirectory;

        /// <summary>
        /// Gets bool value for whether enable customized schema.
        /// </summary>
        [JsonProperty("enableCustomizedSchema")]
        public bool EnableCustomizedSchema { get; set; } = false;

        [JsonProperty("containerRegistry")]
        public ContainerRegistryConfiguration ContainerRegistry { get; set; } = new ContainerRegistryConfiguration();
    }
}
