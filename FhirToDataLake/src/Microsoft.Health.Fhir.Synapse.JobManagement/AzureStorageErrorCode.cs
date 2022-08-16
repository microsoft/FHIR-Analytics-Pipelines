// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public static class AzureStorageErrorCode
    {
        // reference:
        // https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
        // https://docs.microsoft.com/en-us/rest/api/storageservices/queue-service-error-codes
        // https://docs.microsoft.com/en-us/rest/api/storageservices/table-service-error-codes
        public const string GetEntityNotFoundErrorCode = "ResourceNotFound";
        public const string UpdateEntityPreconditionFailedErrorCode = "UpdateConditionNotSatisfied";
        public const string AddEntityAlreadyExistsErrorCode = "EntityAlreadyExists";
        public const string UpdateOrDeleteMessageNotFoundErrorCode = "MessageNotFound";
    }
}