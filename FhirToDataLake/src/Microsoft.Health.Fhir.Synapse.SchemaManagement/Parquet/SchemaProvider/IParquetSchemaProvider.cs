// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public interface IParquetSchemaProvider
    {
        public Task<Dictionary<string, FhirParquetSchemaNode>> GetSchemasAsync(string schemaSource, CancellationToken cancellationToken);
    }
}
