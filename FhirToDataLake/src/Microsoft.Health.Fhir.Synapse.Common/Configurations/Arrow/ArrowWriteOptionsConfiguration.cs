// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow
{
    /// <summary>
    /// Supported Compression type while writing arrow table to parquet.
    /// </summary>
    public enum CompressionOptions
    {
        UNCOMPRESSED,
        SNAPPY,
    }

    public class ArrowWriteOptionsConfiguration
    {
        [JsonProperty("writeBatchSize")]
        public int WriteBatchSize { get; set; } = 100;

        [JsonProperty("compression")]
        public CompressionOptions Compression { get; set; } = CompressionOptions.SNAPPY;
    }
}
