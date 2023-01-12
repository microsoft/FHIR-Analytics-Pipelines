// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Jobs
{
    public enum QueueType : byte
    {
        /// <summary>
        /// FHIR to data lake
        /// </summary>
        FhirToDataLake = 0,

        /// <summary>
        /// Dicom to data lake
        /// </summary>
        DicomToDataLake = 1,
    }
}
