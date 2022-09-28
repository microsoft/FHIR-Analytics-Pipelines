// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public static class AzureStorageKeyProvider
    {
        public static string JobInfoPartitionKey(byte queueType, long groupId) => $"{queueType:D3}_{groupId:D20}";

        public static string JobInfoRowKey(long groupId, long jobId) => $"{groupId:D20}_{jobId:D20}";

        public static string JobLockRowKey(string jobIdentifier) => $"lock_{jobIdentifier}";

        public static string JobReverseIndexPartitionKey(byte queueType, long jobId) => $"{queueType:D3}";

        public static string JobReverseIndexRowKey(byte queueType, long jobId) => $"{queueType:D3}_{jobId:D20}";

        public static string JobIdPartitionKey(byte queueType) => $"{queueType:D3}_jobid";

        public static string JobIdRowKey(byte queueType) => $"{queueType:D3}_jobid";
    }
}