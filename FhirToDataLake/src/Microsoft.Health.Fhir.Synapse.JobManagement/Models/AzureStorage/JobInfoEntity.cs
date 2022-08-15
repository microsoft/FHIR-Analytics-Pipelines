// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure;
using Azure.Data.Tables;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage
{
    public class JobInfoEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        public long Id { get; set; }

        public int QueueType { get; set; }

        public int Status { get; set; }

        public long GroupId { get; set; }

        public string Definition { get; set; }

        public string Result { get; set; }

        public long? Data { get; set; }

        public bool CancelRequested { get; set; }

        public long Version { get; set; }

        public long Priority { get; set; }

        public DateTime CreateDate { get; set; }

        public DateTime? StartDate { get; set; }

        public DateTime? EndDate { get; set; }

        public DateTime HeartbeatDateTime { get; set; }

        public long HeartbeatTimeoutSec { get; set; }
    }
}