// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure;
using Azure.Data.Tables;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs.Models.AzureStorage
{
    public class JobStatusEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        /// <summary>
        /// Job type.
        /// </summary>
        public JobType JobType { get; set; }

        /// <summary>
        /// Job status.
        /// </summary>
        public string JobStatus { get; set; }

        /// <summary>
        /// Group id of Job
        /// </summary>
        public long GroupId { get; set; } = 0;
    }
}