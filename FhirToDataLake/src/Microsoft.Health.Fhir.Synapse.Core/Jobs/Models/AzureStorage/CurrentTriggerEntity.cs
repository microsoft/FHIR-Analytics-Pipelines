// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure;
using Azure.Data.Tables;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage
{
    public class CurrentTriggerEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        /// <summary>
        /// The sequence id of current trigger
        /// </summary>
        public long TriggerSequenceId { get; set; }

        /// <summary>
        /// The status of current trigger
        /// </summary>
        public TriggerStatus TriggerStatus { get; set; }

        /// <summary>
        /// The start time of current trigger
        /// </summary>
        public DateTimeOffset? TriggerStartTime { get; set; }

        /// <summary>
        /// The end time of current trigger
        /// </summary>
        public DateTimeOffset TriggerEndTime { get; set; }

        /// <summary>
        /// The corresponding orchestrator job id of current trigger
        /// </summary>
        public long OrchestratorJobId { get; set; } = 0;

        /// <summary>
        /// The start offset in DICOM Server changefeed
        /// </summary>
        public long StartOffset { get; set; }

        /// <summary>
        /// The end offset in DICOM Server changefeed
        /// </summary>
        public long EndOffset { get; set; }
    }
}