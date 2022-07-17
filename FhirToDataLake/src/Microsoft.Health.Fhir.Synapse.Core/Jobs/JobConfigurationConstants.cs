// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public static class JobConfigurationConstants
    {
        /// <summary>
        /// The number of patients in each task, used in group filter scope.
        /// </summary>
        public const int NumberOfPatientsPerTask = 100;

        /// <summary>
        /// Time interval to sync job to store.
        /// </summary>
        public const int UploadDataIntervalInSeconds = 30;

        /// <summary>
        /// The cache resources number, will commit the cache to storage if there are more resources than this value in cache.
        /// </summary>
        public const int NumberOfResourcesPerCommit = 10000;

        /// <summary>
        /// The data size of cache in bytes, will commit the cache to storage it the data size is larger than this value.
        /// </summary>
        public const int DataSizeInBytesPerCommit = 512 * 1024 * 1024;
    }
}
