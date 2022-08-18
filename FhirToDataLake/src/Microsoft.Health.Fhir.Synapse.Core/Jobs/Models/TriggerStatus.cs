// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    /// <summary>
    /// The status of a trigger
    /// </summary>
    public enum TriggerStatus
    {
        /// <summary>
        /// New Trigger
        /// </summary>
        New,

        /// <summary>
        /// Running
        /// </summary>
        Running,

        /// <summary>
        /// Completed
        /// </summary>
        Completed,

        /// <summary>
        /// Failed
        /// </summary>
        Failed,

        /// <summary>
        /// Cancelled
        /// </summary>
        Cancelled,
    }
}