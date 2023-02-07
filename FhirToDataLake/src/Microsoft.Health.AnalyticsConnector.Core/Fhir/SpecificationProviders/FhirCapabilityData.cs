// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.AnalyticsConnector.Core.Fhir.SpecificationProviders
{
    public class FhirCapabilityData
    {
        public FhirCapabilityData(Dictionary<string, HashSet<string>> resourceTypeSearchParametersLookup, Dictionary<string, string> searchParameterIdLookup)
        {
            ResourceTypeSearchParametersLookup = resourceTypeSearchParametersLookup;
            SearchParameterIdLookup = searchParameterIdLookup;
        }

        /// <summary>
        /// The FHIR server supported search parameters for each resource type, extracted from FHIR server metadata.
        /// </summary>
        public Dictionary<string, HashSet<string>> ResourceTypeSearchParametersLookup { get; set; }

        /// <summary>
        /// {resourceType}_{searchParameter} to search parameter id defined by
        /// </summary>
        public Dictionary<string, string> SearchParameterIdLookup { get; set; }
    }
}
