// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Azure
{
    public static class AzureStorageConstants
    {
        public const string JobFilename = "_fhirsynapsejob.json";

        public const string ResultFolderName = "result";

        public const string JobStatusFolderName = "job";

        public const string CompletedJobFolderName = "completed";

        public const string FailedJobFolderName = "failed";
    }
}
