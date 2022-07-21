// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter
{
    public interface IDataSchemaConverter
    {
        public JsonBatchData Convert(JsonBatchData inputData, string dataType, CancellationToken cancellationToken);
    }
}
