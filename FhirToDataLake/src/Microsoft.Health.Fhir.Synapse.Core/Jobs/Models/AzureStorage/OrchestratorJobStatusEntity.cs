// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Azure;
using Azure.Data.Tables;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage
{
    public class OrchestratorJobStatusEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        

        /// <summary>
        /// statistic result of orchestrator job.
        /// </summary>
        public string StatisticResult { get; set; }

        /// <summary>
        /// The corresponding orchestrator job id of current trigger
        /// </summary>
        public long GroupId { get; set; } = 0;

        public bool IsIncrementalEntity { get; set; } = true;
    }
}