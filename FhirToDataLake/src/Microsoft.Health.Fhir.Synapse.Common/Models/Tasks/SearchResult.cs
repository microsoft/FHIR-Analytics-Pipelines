// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Tasks
{
    public class SearchResult
    {
        public SearchResult(
            List<JObject> fhirResources,
            string continuationToken)
        {
            FhirResources = fhirResources;
            ContinuationToken = continuationToken;
        }

        public List<JObject> FhirResources { get; }

        public string ContinuationToken { get; }
    }
}
