// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class Operations
    {
        /// <summary>
        /// The stage to create job.
        /// </summary>
        public static string CreateJob => nameof(CreateJob);

        /// <summary>
        /// The stage to load FHIR Schema.
        /// </summary>
        public static string LoadFhirSchema => nameof(LoadFhirSchema);

        /// <summary>
        /// The stage to load FHIR resource from FHIR server.
        /// </summary>
        public static string LoadFhirResources => nameof(LoadFhirResources);

        /// <summary>
        /// The stage to convert FHIR data to parquet format.
        /// </summary>
        public static string ProcessData => nameof(ProcessData);

        /// <summary>
        /// The stage to upload data to datalake.
        /// </summary>
        public static string UploadToDatalake => nameof(UploadToDatalake);

        /// <summary>
        /// The stage of health check.
        /// </summary>
        public static string HealthCheck => nameof(HealthCheck);

        /// <summary>
        /// The stage of complete job.
        /// </summary>
        public static string CompleteJob => nameof(CompleteJob);
    }
}
