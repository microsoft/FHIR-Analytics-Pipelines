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
        /// Gets or sets Azure Container Registry image reference of customized schema templates.
        /// </summary>
        [JsonProperty("customizedSchemaImageReference")]
        public string CustomizedSchemaImageReference { get; set; } = null;
    }
}
