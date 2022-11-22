// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class SearchResult
    {
        public SearchResult(
            List<JObject> resources,
            int resultSizeInBytes,
            string continuationToken)
        {
            this.Resources = resources;
            ResultSizeInBytes = resultSizeInBytes;
            ContinuationToken = continuationToken;
        }

        public List<JObject> Resources { get; }

        public int ResultSizeInBytes { get; }

        public string ContinuationToken { get; }
    }
}
