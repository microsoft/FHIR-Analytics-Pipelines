// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.Models.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor.Json
{
    public interface IJsonDataProcessor
    {
        /// <summary>
        /// Process the input data based on Fhir schema.
        /// The purpose of processing is to cast input data based on given FhirSchemaNode instance.
        /// Currently our process tasks contains:
        /// 1. Wrap up some data fields with their children fields to single JSON string.
        /// 2. Add additional choice properties for choice type data. E.g. "Patient.deceasedBoolean" in raw JSON data will become "Patient.deceased.boolean".
        /// 3. Filter out the fields that are not defined in the schema and the fields that does not align with to the schema definition.
        /// </summary>
        /// <param name="inputData">The input JSON data.</param>
        /// <param name="context">Task context.</param>
        /// <param name="cancellationToken">CancellationToken in this operation.</param>
        /// <returns>Batch data with processed values. Null values will be return if there is no valid processed result.</returns>
        public Task<JsonBatchData> ProcessAsync(
            JsonBatchData inputData,
            TaskContext context,
            CancellationToken cancellationToken = default);
    }
}
