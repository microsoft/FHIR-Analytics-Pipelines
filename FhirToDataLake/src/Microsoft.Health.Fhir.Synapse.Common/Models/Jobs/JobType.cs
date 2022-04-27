// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Jobs
{
    /// <summary>
    /// Supported types to process FHIR data.
    /// </summary>
    public enum JobType
    {
        /// <summary>
        /// Process all data from a FHIR server whether or not it is associated with a patient
        /// </summary>
        System = 0,

        /// <summary>
        /// Process data of various types pretained to all patients.
        /// </summary>
        Patient = 1,

        /// <summary>
        /// Process data of various types pretained to all patients in a given group.
        /// </summary>
        Group = 2,
    }
}
