// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    /// <summary>
    /// Constants for FHIR search API parameters.
    /// See https://www.hl7.org/fhir/STU3/search.html#2.21.1.1.
    /// </summary>
    public static class FhirApiConstants
    {
        /// <summary>
        /// Last updated search parameter.
        /// </summary>
        public const string LastUpdatedKey = "_lastUpdated";

        /// <summary>
        /// Page count search parameter.
        /// </summary>
        public const string PageCountKey = "_count";

        /// <summary>
        /// Continuation token search parameter.
        /// </summary>
        public const string ContinuationKey = "ct";

        /// <summary>
        /// Sort search parameter.
        /// </summary>
        public const string SortKey = "_sort";

        /// <summary>
        /// How many resources should be returned in a single page.
        /// Currently _count is limited to 1000 in FHIR server.
        /// https://docs.microsoft.com/en-us/azure/healthcare-apis/fhir/overview-of-search#search-result-parameters
        /// </summary>
        public const int PageCount = 1000;
    }
}
