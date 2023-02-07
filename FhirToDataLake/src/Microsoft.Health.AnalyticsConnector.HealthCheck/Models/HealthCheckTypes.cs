// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.HealthCheck.Models
{
    public static class HealthCheckTypes
    {
        public const string AzureBlobStorageCanReadWrite = "AzureBlobStorage:CanReadWrite";
        public const string FhirServiceCanRead = "FhirService:CanRead";
        public const string DicomServiceCanRead = "DicomService:CanRead";
        public const string AzureContainerRegistryCanRead = "AzureContainerRegistry:CanRead";
        public const string SchedulerServiceIsActive = "SchedulerService:IsActive";
    }
}
