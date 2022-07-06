// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    public enum TaskStage
    {
        /// <summary>
        /// New task.
        /// </summary>
        New,

        /// <summary>
        /// Get newly join patient resources, required in "Group" filter scope.
        /// </summary>
        GetPatientResourceFull,

        /// <summary>
        /// Get updated patient resources for processed patients, required in "Group" filter scope.
        /// </summary>
        GetPatientResourceIncremental,

        /// <summary>
        /// Get resources for "System" filter scope, or get compartment resources for "Group" filter scope.
        /// </summary>
        GetResources,

        /// <summary>
        /// The task is completed.
        /// </summary>
        Completed,
    }
}
