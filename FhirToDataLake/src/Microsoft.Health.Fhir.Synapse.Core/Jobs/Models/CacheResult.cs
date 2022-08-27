// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    /// <summary>
    /// Cache the resources retrieved from Fhir server and search progress in memory,
    /// so we can process the resources and save them to blob storage in batch.
    /// </summary>
    public class CacheResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheResult"/> class based on the provided resources and searchProgress.
        /// </summary>
        /// <param name="cacheSize">the cached data size.</param>
        /// <param name="resources">the resources.</param>
        public CacheResult(
            int cacheSize = 0,
            Dictionary<string, List<JObject>> resources = null)
        {
            CacheSize = cacheSize;
            Resources = resources ?? new Dictionary<string, List<JObject>>();
        }

        /// <summary>
        /// The fhir resources of each resource type
        /// </summary>
        public Dictionary<string, List<JObject>> Resources { get; set; }

        /// <summary>
        /// The data size of cached resources in bytes.
        /// </summary>
        public int CacheSize { get; set; }

        /// <summary>
        /// Get the resource count in cache.
        /// </summary>
        /// <returns>The resource count.</returns>
        public int GetResourceCount()
        {
            return Resources.Values.Sum(resources => resources.Count);
        }

        public void ClearCache()
        {
            Resources.Clear();
            CacheSize = 0;
        }
    }
}
