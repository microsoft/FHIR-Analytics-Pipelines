// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Jobs
{
    public enum JobStatus
    {
        /// <summary>
        /// Job has just been created.
        /// </summary>
        New,

        /// <summary>
        /// Job has started Running.
        /// </summary>
        Running,

        /// <summary>
        /// Job has succeeded.
        /// </summary>
        Succeeded,

        /// <summary>
        /// Job has failed.
        /// </summary>
        Failed,

        /// <summary>
        /// Job has been cancelled.
        /// To do: cancelling is not supported for now.
        /// </summary>
        Cancelled,
    }
}
