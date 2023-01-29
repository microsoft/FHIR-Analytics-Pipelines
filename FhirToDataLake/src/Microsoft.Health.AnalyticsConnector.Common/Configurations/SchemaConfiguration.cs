// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations
{
    public class SchemaConfiguration
    {
        /// <summary>
        /// Gets or sets bool value for whether enable customized schema.
        /// </summary>
        [JsonProperty("enableCustomizedSchema")]
        public bool EnableCustomizedSchema { get; set; } = false;

        /// <summary>
        /// Gets Azure Container Registry image reference of customized schema templates.
        /// </summary>
        [JsonProperty("schemaImageReference")]
        public string SchemaImageReference { get; set; } = string.Empty;
    }
}
