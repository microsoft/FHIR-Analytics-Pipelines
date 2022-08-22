// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage
{
    public static class JobInfoEntityProperties
    {
        public const string Id = "Id";
        public const string QueueType = "QueueType";
        public const string Status = "Status";
        public const string GroupId = "GroupId";
        public const string Definition = "Definition";
        public const string Result = "Result";
        public const string Data = "Data";
        public const string CancelRequested = "CancelRequested";
        public const string Version = "Version";
        public const string Priority = "Priority";
        public const string CreateDate = "CreateDate";
        public const string StartDate = "StartDate";
        public const string EndDate = "EndDate";
        public const string HeartbeatDateTime = "HeartbeatDateTime";

        // new property
        public const string HeartbeatTimeoutSec = "HeartbeatTimeoutSec";
    }
}