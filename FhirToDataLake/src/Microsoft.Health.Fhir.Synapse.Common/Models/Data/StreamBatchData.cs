// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Data
{
    /// <summary>
    /// Batch data serialized to Stream format.
    /// </summary>
    public class StreamBatchData : IDisposable
    {
        public StreamBatchData(Stream value, int batchSize, string schemaType)
        {
            Value = value;
            BatchSize = batchSize;
            SchemaType = schemaType;
        }

        public Stream Value { get; set; }

        public int BatchSize { get; set; }

        public string SchemaType { get; set; }

        public void Dispose()
        {
            Value.Dispose();
        }
    }
}
