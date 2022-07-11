// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure;
using Azure.Data.Tables;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class TriggerIdEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        public long TriggerId { get; set; }

        public TriggerStatus Status { get; set; }

        public DateTime TriggerTimeStart { get; set; }

        public DateTime TriggerTimeEnd { get; set; }

        public long OrchestratorJobId { get; set; }
    }
}
