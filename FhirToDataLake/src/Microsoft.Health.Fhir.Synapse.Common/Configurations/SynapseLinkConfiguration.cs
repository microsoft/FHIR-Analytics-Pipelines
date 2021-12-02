// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class SynapseLinkConfiguration
    {
        [JsonProperty("fhirServer")]
        public FhirServerConfiguration FhirServer { get; set; } = new FhirServerConfiguration();

        [JsonProperty("dataLakeStore")]
        public DataLakeStoreConfiguration DataLakeStore { get; set; } = new DataLakeStoreConfiguration();

        [JsonProperty("job")]
        public JobConfiguration Job { get; set; } = new JobConfiguration();

        [JsonProperty("scheduler")]
        public JobSchedulerConfiguration Scheduler { get; set; } = new JobSchedulerConfiguration();

        [JsonProperty("arrow")]
        public ArrowConfiguration Arrow { get; set; } = new ArrowConfiguration();
    }
}
