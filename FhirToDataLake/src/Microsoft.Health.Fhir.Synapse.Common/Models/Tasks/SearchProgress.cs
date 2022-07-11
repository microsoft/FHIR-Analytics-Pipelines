// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    /// <summary>
    /// Search progress of a task.
    /// </summary>
    public class SearchProgress
    {
        public SearchProgress(
            int currentIndex = 0,
            int currentFilter = 0,
            string continuationToken = null,
            bool isCurrentSearchCompleted = false,
            Dictionary<string, int> patientVersionId = null)
        {
            CurrentIndex = currentIndex;
            CurrentFilter = currentFilter;
            ContinuationToken = continuationToken;
            IsCurrentSearchCompleted = isCurrentSearchCompleted;
            PatientVersionId = patientVersionId ?? new Dictionary<string, int>();
        }

        /// <summary>
        /// Current compartment index for processing, used for group filter scope.
        /// </summary>
        [JsonProperty("currentIndex")]
        public int CurrentIndex { get; set; }

        /// <summary>
        /// Current type filter index for processing
        /// </summary>
        [JsonProperty("currentFilter")]
        public int CurrentFilter { get; set; }

        /// <summary>
        /// Continuation token of the current search.
        /// </summary>
        [JsonProperty("continuationToken")]
        public string ContinuationToken { get; set; }

        /// <summary>
        /// Has completed the current search.
        /// </summary>
        [JsonProperty("isCurrentSearchCompleted")]
        public bool IsCurrentSearchCompleted { get; set; }

        /// <summary>
        /// The version id of processed patients
        /// </summary>
        [JsonProperty("patientVersionId")]
        public Dictionary<string, int> PatientVersionId { get; set; }

        /// <summary>
        /// Update CurrentIndex the the specified index.
        /// </summary>
        /// <param name="index">the target index.</param>
        public void UpdateCurrentIndex(int index)
        {
            CurrentIndex = index;
            CurrentFilter = 0;
            ClearContinuationToken();
        }

        /// <summary>
        /// Update CurrentFilter to the specified index.
        /// </summary>
        /// <param name="index">the target index.</param>
        public void UpdateCurrentFilter(int index)
        {
            CurrentFilter = index;
            ClearContinuationToken();
        }

        /// <summary>
        /// Set continuation token to null and set IsCurrentSearchCompleted to false.
        /// </summary>
        private void ClearContinuationToken()
        {
            ContinuationToken = null;
            IsCurrentSearchCompleted = false;
        }
    }
}
