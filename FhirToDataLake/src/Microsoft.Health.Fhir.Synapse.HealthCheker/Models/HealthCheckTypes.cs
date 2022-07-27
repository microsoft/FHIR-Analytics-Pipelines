// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Models
{
    public static class HealthCheckTypes
    {
        public const string AzureBlobStorageCanReadWriteDelete = "AzureBlobStorage:CanReadWriteDelete";
        public const string FhirServiceCanSearch = "FhirService:CanSearch";
    }
}
