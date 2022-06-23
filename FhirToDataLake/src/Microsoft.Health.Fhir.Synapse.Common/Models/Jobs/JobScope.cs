// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Jobs
{
    /// <summary>
    /// Supported types to process FHIR data.
    /// </summary>
    public enum JobScope
    {
        /// <summary>
        /// Process all data from a FHIR server whether or not it is associated with a patient
        /// </summary>
        System = 0,

        /// <summary>
        /// Process data of various types pretained to all patients in a given group.
        /// </summary>
        Group = 1,

        /// <summary>
        /// Process data of various types pretained to all patients.
        /// </summary>
        /// To do: patient is not supported for now.
        // Patient = 2,
    }
}
