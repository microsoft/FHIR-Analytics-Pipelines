// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    public class CacheResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheResult"/> class.
        /// </summary>
        public CacheResult()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheResult"/> class based on the provided resources and taskProgress.
        /// </summary>
        /// <param name="resources">the resources.</param>
        /// <param name="searchProgress">the search progress.</param>
        public CacheResult(
            SearchProgress searchProgress,
            Dictionary<string, List<JObject>> resources = null)
        {
            Resources = resources ?? new Dictionary<string, List<JObject>>();
            SearchProgress = searchProgress ?? new SearchProgress();
        }

        public Dictionary<string, List<JObject>> Resources { get; set; } = new Dictionary<string, List<JObject>>();

        public SearchProgress SearchProgress { get; set; } = new SearchProgress();

        public int GetResourceCount()
        {
            return Resources.Values.Sum(resources => resources.Count);
        }
    }
}
