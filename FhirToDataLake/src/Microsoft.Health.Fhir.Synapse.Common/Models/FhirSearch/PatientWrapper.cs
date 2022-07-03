// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch
{
    public class PatientWrapper
    {
        public PatientWrapper(
            string patientId,
            bool isNewPatient = true)
        {
            PatientId = patientId;
            IsNewPatient = isNewPatient;
        }

        [JsonProperty("patientId")]
        public string PatientId { get; }

        [JsonProperty("isNewPatient")]
        public bool IsNewPatient { get; set; }
    }
}
