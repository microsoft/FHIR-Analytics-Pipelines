// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.JobManagement
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
        public const string RequestBodyTooLargeErrorCode = "RequestBodyTooLarge";
        public const string PropertyValueTooLargeErrorCode = "PropertyValueTooLarge";
        public const string InvalidDuplicateRowErrorCode = "InvalidDuplicateRow";
        public const string UpdateOrDeleteMessageNotFoundErrorCode = "MessageNotFound";
        public const string PopReceiptMismatchErrorCode = "PopReceiptMismatch";
        public const string NoAuthenticationInformationErrorCode = "NoAuthenticationInformation";
        public const string InvalidAuthenticationInfoErrorCode = "InvalidAuthenticationInfo";
        public const string AuthenticationFailedErrorCode = "AuthenticationFailed";

        // the error for 403 in doc is "AuthenticationFailed", while it throws "AuthorizationFailure" actually
        public const string AuthorizationFailureErrorCode = "AuthorizationFailure";
    }
}