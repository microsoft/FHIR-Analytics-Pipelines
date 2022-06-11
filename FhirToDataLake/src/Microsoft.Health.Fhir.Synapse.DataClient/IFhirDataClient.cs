// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.SearchOption;

namespace Microsoft.Health.Fhir.Synapse.DataClient
{
    public interface IFhirDataClient
    {
        /// <summary>
        /// Returns a FHIR bundle which contains the matching search results.
        /// </summary>
        /// <param name="fhirSearchOptions">search options.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>returned bundle.</returns>
        public Task<string> SearchAsync(
            BaseSearchOptions fhirSearchOptions,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Returns a FHIR bundle which contains the matching search results.
        /// </summary>
        /// <param name="searchParameters">search parameters.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>returned bundle.</returns>
        public Task<string> SearchAsync(
            FhirSearchParameters searchParameters,
            CancellationToken cancellationToken = default);

        public Task<string> SearchCompartmentAsync(
            string PatientId,
            FhirSearchParameters searchParameters,
            CancellationToken cancellationToken = default);
    }
}
