// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor
{
    public interface IColumnDataProcessor
    {
        /// <summary>
        /// Process Json input data into stream output data.
        /// </summary>
        /// <param name="inputData">The input JSON data.</param>
        /// <param name="context">Task context.</param>
        /// <param name="cancellationToken">CancellationToken in this operation.</param>
        /// <returns>DataItem with Stream value. Null stream value will be return if there is no valid processed result.</returns>
        public Task<StreamBatchData> ProcessAsync(
            JsonBatchData inputData,
            TaskContext context,
            CancellationToken cancellationToken = default);
    }
}
