// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations
{
    public class DataSourceConfiguration
    {
        [JsonProperty("type")]
        public DataSourceType Type { get; set; } = DataSourceType.FHIR;

        [JsonProperty("fhirServer")]
        public FhirServerConfiguration FhirServer { get; set; } = new FhirServerConfiguration();

        [JsonProperty("dicomServer")]
        public DicomServerConfiguration DicomServer { get; set; } = new DicomServerConfiguration();
    }
}
