// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.DicomApiOption;

namespace Microsoft.Health.Fhir.Synapse.DataClient
{
    public interface IDicomDataClient
    {
        /// <summary>
        /// Returns server reponse content contains matching search results.
        /// </summary>
        /// <param name="dicomApiOptions">dicom api options.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>returned bundle.</returns>
        public Task<string> SearchAsync(
            BaseDicomApiOptions dicomApiOptions,
            CancellationToken cancellationToken = default);
    }
}
