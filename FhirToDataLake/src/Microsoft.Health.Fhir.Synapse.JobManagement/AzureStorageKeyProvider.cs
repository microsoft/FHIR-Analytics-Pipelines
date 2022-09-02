// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public static class AzureStorageKeyProvider
    {
        // The table name must conform to rules: https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#table-names
        public static string JobInfoTableName(string agentName) => $"{agentName.ToLower()}jobinfotable";

        // The queue name must conform to rules: https://docs.microsoft.com/en-us/rest/api/storageservices/naming-queues-and-metadata#queue-names
        public static string JobMessageQueueName(string agentName) => $"{agentName.ToLower()}jobinfoqueue";

        public static string JobInfoPartitionKey(byte queueType, long groupId) => $"{queueType:D3}_{groupId:D20}";

        public static string JobInfoRowKey(long groupId, long jobId) => $"{groupId:D20}_{jobId:D20}";

        public static string JobLockRowKey(string jobIdentifier) => $"lock_{jobIdentifier}";

        public static string JobReverseIndexPartitionKey(byte queueType, long jobId) => $"{queueType:D3}";

        public static string JobReverseIndexRowKey(byte queueType, long jobId) => $"{queueType:D3}_{jobId:D20}";

        public static string JobIdPartitionKey(byte queueType) => $"{queueType:D3}_jobid";

        public static string JobIdRowKey(byte queueType) => $"{queueType:D3}_jobid";
    }
}