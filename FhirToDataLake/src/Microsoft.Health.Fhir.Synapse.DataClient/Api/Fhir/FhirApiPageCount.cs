// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api.Fhir
{
    /// <summary>
    /// Resources count should be returned in a single page
    /// </summary>
    public enum FhirApiPageCount
    {
        /// <summary>
        /// Return single resource
        /// </summary>
        Single = 1,

        /// <summary>
        /// Return batch resouces
        /// Currently _count is limited to 1000 in FHIR server.
        /// https://docs.microsoft.com/en-us/azure/healthcare-apis/fhir/overview-of-search#search-result-parameters
        /// </summary>
        Batch = 1000,
    }
}
