// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;

namespace Microsoft.Health.Fhir.Synapse.DataClient
{
    public interface IFhirDataClient
    {
        /// <summary>
        /// Returns a FHIR bundle which contains the matching search results.
        /// </summary>
        /// <param name="fhirApiOptions">fhir api options.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>returned bundle.</returns>
        public Task<string> SearchAsync(
            BaseFhirApiOptions fhirApiOptions,
            CancellationToken cancellationToken = default);
    }
}