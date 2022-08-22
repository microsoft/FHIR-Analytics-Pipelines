﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public delegate IParquetSchemaProvider ParquetSchemaProviderDelegate(string name);

    public interface IParquetSchemaProvider
    {
        public Task<Dictionary<string, FhirParquetSchemaNode>> GetSchemasAsync(FhirVersion fhirVersion, CancellationToken cancellationToken = default);
    }
}
