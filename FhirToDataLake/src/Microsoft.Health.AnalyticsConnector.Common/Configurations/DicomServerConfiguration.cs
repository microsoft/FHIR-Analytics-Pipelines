// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations
{
    public class DicomServerConfiguration
    {
        [JsonProperty("serverUrl")]
        public string ServerUrl { get; set; } = string.Empty;

        [JsonProperty("apiVersion")]
        public DicomApiVersion ApiVersion { get; set; } = DicomApiVersion.V1;

        [JsonProperty("authentication")]
        public AuthenticationType Authentication { get; set; } = AuthenticationType.None;

        // Azure DICOM server default audience is "https://dicom.healthcareapis.azure.com".
        // Refer https://learn.microsoft.com/en-us/azure/healthcare-apis/dicom/dicom-get-access-token-azure-cli-old#obtain-a-token
        [JsonProperty("audience")]
        public string Audience { get; set; } = "https://dicom.healthcareapis.azure.com";
    }
}
