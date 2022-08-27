// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch
{
    /// <summary>
    /// Wrap the patient information
    /// </summary>
    public class PatientWrapper
    {
        public PatientWrapper(
            string patientHash,
            long versionId = 0)
        {
            PatientHash = patientHash;
            VersionId = versionId;
        }

        /// <summary>
        /// Patient id
        /// </summary>
        [JsonProperty("patientHash")]
        public string PatientHash { get; }

        /// <summary>
        /// Version ID
        /// </summary>
        [JsonProperty("versionId")]
        public long VersionId { get; set; }
    }
}
