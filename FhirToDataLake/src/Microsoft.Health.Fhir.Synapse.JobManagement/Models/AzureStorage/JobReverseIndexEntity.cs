// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure;
using Azure.Data.Tables;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage
{
    public class JobReverseIndexEntity : ITableEntity
    {
        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

        public string JobInfoEntityPartitionKey { get; set; } = string.Empty;

        public string JobInfoEntityRowKey { get; set; } = string.Empty;
    }
}