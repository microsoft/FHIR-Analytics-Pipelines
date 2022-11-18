// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class DicomServerConfiguration
    {
        [JsonProperty("serverUrl")]
        public string ServerUrl { get; set; } = string.Empty;

        [JsonProperty("version")]
        public DicomApiVersion Version { get; set; } = DicomApiVersion.V1;

        [JsonProperty("authentication")]
        public AuthenticationType Authentication { get; set; } = AuthenticationType.None;
    }
}
