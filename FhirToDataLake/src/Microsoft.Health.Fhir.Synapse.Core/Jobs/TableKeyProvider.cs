// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public static class TableKeyProvider
    {
        public static string TriggerPartitionKey(byte queueType) => $"{queueType:D3}_trigger";

        public static string TriggerRowKey(byte queueType) => $"{queueType:D3}_trigger";

        public static string JobStatusPartitionKey(byte queueType, int jobType) => $"{queueType:D3}_{jobType}_jobStatus";

        public static string JobStatusRowKey(byte queueType, int jobType, long groupId) => $"{queueType:D3}_{jobType}_{groupId:D20}";

        public static string LeasePartitionKey(byte queueType) => $"{queueType:D3}_lock";

        public static string LeaseRowKey(byte queueType) => $"{queueType:D3}_lock";

        public static string CompartmentPartitionKey(byte queueType) => $"compartmentinfo_{queueType:D3}";

        public static string CompartmentRowKey(string patientId) => $"{patientId.ComputeHash()}";
    }
}