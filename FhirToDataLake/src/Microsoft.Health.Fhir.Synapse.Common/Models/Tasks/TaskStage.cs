// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    public enum TaskStage
    {
        /// <summary>
        /// A task is new created
        /// </summary>
        New,

        /// <summary>
        /// Get newly join patient resources, required in "Group" job scope.
        /// </summary>
        GetNewPatient,

        /// <summary>
        /// Get updated patient resources for processed patients, required in "Group" job scope.
        /// </summary>
        GetUpdatedPatient,

        /// <summary>
        /// Get resources for "System" job scope, or get compartment resources for "Group" job scope.
        /// </summary>
        GetResources,

        /// <summary>
        /// The task is finished.
        /// </summary>
        Finished,
    }
}
