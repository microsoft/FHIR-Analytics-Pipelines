// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter
{
    public delegate IDataSchemaConverter DataSchemaConverterDelegate(string name);

    public interface IDataSchemaConverter
    {
        /// <summary>
        /// Convert the input data to target data type structure.
        /// </summary>
        /// <param name="inputData">Input data instance.</param>
        /// <param name="dataType">Target data type.</param>
        /// <param name="cancellationToken">Method cancellationToken.</param>
        /// <returns>Converted data instance.</returns>
        public JsonBatchData Convert(JsonBatchData inputData, string dataType, CancellationToken cancellationToken);
    }
}
