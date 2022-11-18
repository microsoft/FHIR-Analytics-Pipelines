// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class ErrorType
    {
        /// <summary>
        /// A metric type for errors that occur when creating job.
        /// </summary>
        public static string CreateJobError => nameof(CreateJobError);

        /// <summary>
        /// A metric type for authentication errors
        /// </summary>
        public static string AuthenticationError => nameof(AuthenticationError);

        /// <summary>
        /// A metric type for FHIR server errors
        /// </summary>
        public static string ReadFhirDataError => nameof(ReadFhirDataError);

        /// <summary>
        /// A metric type for data lake errors
        /// </summary>
        public static string WriteToDatalakeError => nameof(WriteToDatalakeError);

        /// <summary>
        /// A metric type for errors that occur when processing data from FHIR to parquet format.
        /// </summary>
        public static string DataProcessError => nameof(DataProcessError);

        /// <summary>
        /// A metric type for errors that occur when loading or parsing the FHIR template json file.
        /// </summary>
        public static string FhirSchemaError => nameof(FhirSchemaError);

        /// <summary>
        /// A metric type for errors that occur when getting filters.
        /// </summary>
        public static string ReadFilterError => nameof(ReadFilterError);

        /// <summary>
        /// A metric type for errors that occur when job execution failed.
        /// </summary>
        public static string JobExecutionError => nameof(JobExecutionError);

        /// <summary>
        /// A metric type for errors that occur in scheduler service.
        /// </summary>
        public static string SchedulerServiceError => nameof(SchedulerServiceError);

        /// <summary>
        /// A metric type for errors that occur in health check.
        /// </summary>
        public static string HealthCheckError => nameof(HealthCheckError);
    }
}
