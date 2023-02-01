// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.AnalyticsConnector.JobManagement.Extensions;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public static class TableKeyProvider
    {
        public static string TriggerPartitionKey(byte queueType) => $"{queueType:D3}_trigger";

        public static string TriggerRowKey(byte queueType) => $"{queueType:D3}_trigger";

        public static string LeasePartitionKey(byte queueType) => $"{queueType:D3}_lock";

        public static string LeaseRowKey(byte queueType) => $"{queueType:D3}_lock";

        public static string CompartmentPartitionKey(byte queueType) => $"compartmentinfo_{queueType:D3}";

        public static string CompartmentRowKey(string patientId) => $"{patientId.ComputeHash()}";
    }
}