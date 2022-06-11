// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    /// <summary>
    /// Task progress.
    /// </summary>
    public class TaskProgress
    {
        public TaskProgress(
            int currentIndex = 0,
            int currentFilter = 0,
            string continuationToken = null,
            bool isCompleted = false)
        {
            CurrentIndex = currentIndex;
            CurrentFilter = currentFilter;
            ContinuationToken = continuationToken;
            IsCompleted = isCompleted;
        }

        /// <summary>
        /// Has completed all resources.
        /// </summary>
        public bool IsCompleted { get; set; }

        /// <summary>
        /// current resource type/patient index for processing.
        /// </summary>
        public int CurrentIndex { get; set; }

        /// <summary>
        /// current resource filter index for processing,
        /// set to -1 if no resource filters present
        /// </summary>
        public int CurrentFilter { get; set; }

        /// <summary>
        /// continuation token
        /// </summary>
        public string ContinuationToken { get; set; }
    }
}
