// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations
{
    public class FhirDataLakeStoreConfiguration
    {
        [JsonProperty("storageUrl")]
        public string StorageUrl { get; set; } = string.Empty;

        [JsonProperty("containerName")]
        public string ContainerName { get; set; } = string.Empty;

        [JsonProperty("version")]
        public FhirVersion Version { get; set; } = FhirVersion.R4;
    }
}
