// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow
{
    public class ArrowReadOptionsConfiguration
    {
        [JsonProperty("useThreads")]
        public bool UseThreads { get; set; } = true;

        [JsonProperty("blockSize")]
        public int BlockSize { get; set; } = 1 << 20;
    }
}
