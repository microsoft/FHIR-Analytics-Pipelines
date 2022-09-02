// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class StorageConfiguration
    {
        [JsonProperty("externalStorageConnectionString")]
        public string ExternalStorageConnectionString { get; set; } = string.Empty;

        [JsonProperty("internalStorageConnectionString")]
        public string InternalStorageConnectionString { get; set; } = string.Empty;
    }
}
