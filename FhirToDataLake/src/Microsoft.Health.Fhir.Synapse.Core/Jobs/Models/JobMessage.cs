// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    [DataContract]
    public class JobMessage
    {
        public JobMessage(string partitionKey, string rowKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
        }

        [JsonProperty("pk")]
        public string PartitionKey { get; set; }

        [JsonProperty("rk")]
        public string RowKey { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

        public static JobMessage Parse(string text)
        {
            return JsonConvert.DeserializeObject<JobMessage>(text);
        }
    }
}
