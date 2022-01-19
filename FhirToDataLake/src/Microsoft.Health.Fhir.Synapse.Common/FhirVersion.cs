// -------------------------------------------------------------------------------------------------
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
        // Currently we only support FHIR R4.
        R4,

        // Stu3 is not supported.
        Stu3,
    }
}
