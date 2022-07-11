// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.Core.Extensions;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public static class JobTableKeyProvider
    {
        public static string TriggerPartitionKey(byte queueType) => $"{queueType:D3}_trigger";

        public static string JobIdPartitionKey(byte queueType) => $"{queueType:D3}_jobid";

        public static string JobPartitionKey(byte queueType, long groupId) => $"{queueType:D3}_{groupId:D20}";

        public static string JobRowKey(long groupId, long jobId) => $"{groupId:D20}_{jobId:D20}";

        public static string JobReverseIndexPartitionKey(byte queueType, long jobId) => $"{queueType:D3}_{jobId:D20}";

        public static string JobLockKey(string jobDefinition) => $"lock_{jobDefinition.ComputeHash()}";
    }
}
