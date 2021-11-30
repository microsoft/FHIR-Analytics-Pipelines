// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Data
{
    /// <summary>
    /// Batch data deserialized to JSON objects.
    /// </summary>
    public class JsonBatchData
    {
        public JsonBatchData(IEnumerable<JObject> values)
        {
            Values = values;
        }

        public IEnumerable<JObject> Values { get; set; }
    }
}
