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
        public StreamBatchData(Stream value)
        {
            Value = value;
        }

        public Stream Value { get; set; }
    }
}
