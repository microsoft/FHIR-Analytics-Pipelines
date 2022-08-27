// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public static class TableKeyProvider
    {
        // The table name must conform to rules: https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#table-names
        public static string MetadataTableName(string agentName) => $"{agentName.ToLower()}metadatatable";

        public static string TriggerPartitionKey(byte queueType) => $"{queueType:D3}_trigger";

        public static string TriggerRowKey(byte queueType) => $"{queueType:D3}_trigger";

        public static string LeasePartitionKey(byte queueType) => $"{queueType:D3}_lock";

        public static string LeaseRowKey(byte queueType) => $"{queueType:D3}_lock";

        public static string CompartmentPartitionKey(byte queueType) => $"compartmentinfo_{queueType:D3}";

        public static string CompartmentRowKey(string patientId) => $"{patientId.ComputeHash()}";
    }
}