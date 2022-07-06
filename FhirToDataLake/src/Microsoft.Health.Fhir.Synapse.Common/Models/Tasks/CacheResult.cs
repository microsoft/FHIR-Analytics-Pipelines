// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    /// <summary>
    /// Cache the resources retrieved from Fhir server in memory,
    /// so we can process the resources and save them to blob storage in batch.
    /// </summary>
    public class CacheResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheResult"/> class based on the provided resources and taskProgress.
        /// </summary>
        /// <param name="searchProgress">the search progress.</param>
        /// <param name="resources">the resources.</param>
        public CacheResult(
            SearchProgress searchProgress = null,
            Dictionary<string, List<JObject>> resources = null)
        {
            SearchProgress = searchProgress ?? new SearchProgress();
            Resources = resources ?? new Dictionary<string, List<JObject>>();
        }

        /// <summary>
        /// The fhir resources of each resource type
        /// </summary>
        public Dictionary<string, List<JObject>> Resources { get; set; }

        /// <summary>
        /// The search progress, which is consistent with the cached resources.
        /// It will be committed to taskContext when commits cache resources to blob storage.
        /// </summary>
        public SearchProgress SearchProgress { get; set; }

        /// <summary>
        /// Get the resource count in cache.
        /// </summary>
        /// <returns>The resource count.</returns>
        public int GetResourceCount()
        {
            return Resources.Values.Sum(resources => resources.Count);
        }
    }
}
