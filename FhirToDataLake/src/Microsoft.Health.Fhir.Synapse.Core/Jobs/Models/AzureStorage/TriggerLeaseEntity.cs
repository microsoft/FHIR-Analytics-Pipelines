// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure;
using Azure.Data.Tables;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage
{
    public class TriggerLeaseEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        /// <summary>
        /// The Guid of working instance
        /// </summary>
        public Guid WorkingInstanceGuid { get; set; }

        public DateTimeOffset HeartbeatDateTime { get; set; }
    }
}