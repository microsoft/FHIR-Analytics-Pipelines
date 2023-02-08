// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.JobManagement.Models.AzureStorage
{
    [DataContract]
    public class JobMessage
    {
        public JobMessage(string partitionKey, string rowKey, string lockRowKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            LockRowKey = lockRowKey;
        }

        [JsonProperty("pk")]
        public string PartitionKey { get; set; }

        [JsonProperty("rk")]
        public string RowKey { get; set; }

        [JsonProperty("lockrk")]
        public string LockRowKey { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

        /// <summary>
        /// Parse string to JobMessage
        /// </summary>
        /// <param name="text">the string to be parsed</param>
        /// <returns>JobMessage, return null if error</returns>
        public static JobMessage? Parse(string text)
        {
            try
            {
                return JsonConvert.DeserializeObject<JobMessage>(text);
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}