// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.Common.Models.Jobs
{
    /// <summary>
    /// Supported filter scope to process FHIR data.
    /// </summary>
    public enum FilterScope
    {
        /// <summary>
        /// Process all data from a FHIR server whether or not it is associated with a patient
        /// </summary>
        System = 0,

        /// <summary>
        /// Process data of various types pertained to all patients in a given group.
        /// </summary>
        Group = 1,
    }
}
