// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch
{
    /// <summary>
    /// Wrap the patient information used for Fhir search
    /// </summary>
    public class PatientWrapper
    {
        public PatientWrapper(
            string patientId,
            long versionId = 0)
        {
            PatientId = patientId;
            VersionId = versionId;
        }

        /// <summary>
        /// Patient id
        /// </summary>
        [JsonProperty("patientId")]
        public string PatientId { get; }

        /// <summary>
        /// Version ID
        /// </summary>
        [JsonProperty("versionId")]
        public long VersionId { get; set; }
    }
}
