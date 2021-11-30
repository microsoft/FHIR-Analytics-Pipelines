// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow
{
    public class ArrowConfiguration
    {
        [JsonProperty("readOptions")]
        public ArrowReadOptionsConfiguration ReadOptions { get; set; } = new ArrowReadOptionsConfiguration();

        [JsonProperty("parseOptions")]
        public ArrowParseOptionsConfiguration ParseOptions { get; set; } = new ArrowParseOptionsConfiguration();

        [JsonProperty("writeOptions")]
        public ArrowWriteOptionsConfiguration WriteOptions { get; set; } = new ArrowWriteOptionsConfiguration();
    }
}
