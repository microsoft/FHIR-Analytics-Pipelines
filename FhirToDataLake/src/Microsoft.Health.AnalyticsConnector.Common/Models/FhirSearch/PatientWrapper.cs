// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;
using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Common.Models.FhirSearch
{
    /// <summary>
    /// Wrap the patient information
    /// </summary>
    public class PatientWrapper
    {
        public PatientWrapper(
            string patientHash,
            long versionId)
        {
            PatientHash = EnsureArg.IsNotNullOrWhiteSpace(patientHash, nameof(patientHash));
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