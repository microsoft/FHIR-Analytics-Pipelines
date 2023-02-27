// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs.Models
{
    public class FhirDataLakeToDataLakeProcessingJobInputData
    {
        /// <summary>
        /// Job type
        /// </summary>
        public JobType JobType { get; set; }

        /// <summary>
        /// Job version, used for version update
        /// </summary>
        public JobVersion JobVersion { get; set; } = FhirDataLakeToDataLakeJobVersionManager.DefaultJobVersion;

        /// <summary>
        /// Trigger sequence id
        /// </summary>
        public long TriggerSequenceId { get; set; }

        /// <summary>
        /// Blob name
        /// </summary>
        public string BlobName { get; set; }

        /// <summary>
        /// ETag
        /// </summary>
        public string ETag { get; set; }
    }
}