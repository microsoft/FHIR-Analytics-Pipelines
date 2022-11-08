﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common
{
    /// <summary>
    /// Supported fhir versions.
    /// </summary>
    public enum FhirVersion
    {
        /// <summary>
        /// R5
        /// </summary>
        R5,

        /// <summary>
        /// R4
        /// </summary>
        R4,

        /// <summary>
        /// Stu3 is not supported.
        /// </summary>
        Stu3,

        /// <summary>
        /// DICOM
        /// </summary>
        DICOM,
    }
}
