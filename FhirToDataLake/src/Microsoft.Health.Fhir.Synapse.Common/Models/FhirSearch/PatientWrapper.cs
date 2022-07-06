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
            bool isNewPatient = true)
        {
            PatientId = patientId;
            IsNewPatient = isNewPatient;
        }

        /// <summary>
        /// Patient id
        /// </summary>
        [JsonProperty("patientId")]
        public string PatientId { get; }

        /// <summary>
        /// Is new patient
        /// </summary>
        [JsonProperty("isNewPatient")]
        public bool IsNewPatient { get; set; }
    }
}
