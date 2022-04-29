// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.IO;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Data
{
    /// <summary>
    /// Batch data serialized to Stream format.
    /// </summary>
    public class StreamBatchData
    {
        public StreamBatchData(Stream value, int count, string schemaType)
        {
            Value = value;
            Count = count;
            SchemaType = schemaType;
        }

        public Stream Value { get; set; }

        public int Count { get; set; }

        public string SchemaType { get; set; }
    }
}
