// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;

namespace Microsoft.Health.Fhir.Synapse.Common
{
    public interface IAnonymizationProcessor
    {
        /// <summary>
        /// Process Json input data into stream output data.
        /// </summary>
        /// <param name="inputData">DataItem with ITypedElement values.</param>
        /// <param name="cancellationToken">CancellationToken.</param>
        /// <returns>DataIime with ITypedElement values.</returns>
        public Task<FhirElementBatchData> ProcessAsync(
            FhirElementBatchData inputData,
            CancellationToken cancellationToken = default);
    }
}
