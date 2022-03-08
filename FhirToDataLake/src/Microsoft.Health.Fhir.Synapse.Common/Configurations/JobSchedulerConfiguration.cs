// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations
{
    public class JobSchedulerConfiguration
    {
        /// <summary>
        /// How many resource types to proceed in parallel.
        /// </summary>
        [JsonProperty("maxConcurrencyCount")]
        public int MaxConcurrencyCount { get; set; } = 10;
    }
}
