// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure;
using Azure.Data.Tables;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs.Models.AzureStorage
{
    public class CompartmentInfoEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        /// <summary>
        /// The version id of patient
        /// </summary>
        public long VersionId { get; set; }
    }
}