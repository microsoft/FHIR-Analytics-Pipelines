// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Data.Tables;
using Azure.Storage.Queues;

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public interface IAzureStorageClientFactory
    {
        public TableClient CreateTableClient();

        public QueueClient CreateQueueClient();
    }
}