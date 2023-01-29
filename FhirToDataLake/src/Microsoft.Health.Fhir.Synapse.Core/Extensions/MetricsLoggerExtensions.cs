// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.Core.Extensions
{
    public static class MetricsLoggerExtensions
    {
        public static void LogTotalErrorsMetrics(this IMetricsLogger metricsLogger, Exception ex, string message, string operationType)
        {
            switch (operationType)
            {
                case nameof(Operations.RunSchedulerService):
                    metricsLogger.LogMetrics(new TotalErrorsMetric(false, ErrorType.SchedulerServiceError, message, Operations.RunSchedulerService), 1);
                    break;
                case nameof(Operations.HealthCheck):
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.HealthCheckError, message, Operations.HealthCheck), 1);
                    break;
                case nameof(Operations.CreateJob):
                    metricsLogger.LogMetrics(new TotalErrorsMetric(false, ErrorType.CreateJobError, message, Operations.CreateJob), 1);
                    break;
                case nameof(Operations.RunJob):
                    ProcessJobExecutionError(metricsLogger, ex, message);
                    break;
                default:
                    throw new ConfigurationErrorException(
                        $"Error operation type {operationType} is not supported.");
            }
        }

        public static void LogSuccessfulResourceCountMetric(this IMetricsLogger metricsLogger, double value)
        {
            metricsLogger.LogMetrics(new SuccessfulResourceCountMetric(), value);
        }

        public static void LogSuccessfulDataSizeMetric(this IMetricsLogger metricsLogger, double value)
        {
            metricsLogger.LogMetrics(new SuccessfulDataSizeMetric(), value);
        }

        public static void LogResourceLatencyMetric(this IMetricsLogger metricsLogger, double value)
        {
            metricsLogger.LogMetrics(new ResourceLatencyMetric(), value);
        }

        public static void LogHealthStatusMetric(this IMetricsLogger metricsLogger, string component, bool isDiagnostic, double value)
        {
            metricsLogger.LogMetrics(new HealthStatusMetric(component, isDiagnostic), value);
        }

        private static void ProcessJobExecutionError(IMetricsLogger metricsLogger, Exception ex, string message)
        {
            switch (ex)
            {
                case FhirSearchException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.ReadFhirDataError, message, Operations.RunJob), 1);
                    break;
                case AzureBlobOperationFailedException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.WriteToDatalakeError, message, Operations.RunJob), 1);
                    break;
                case FhirSchemaException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.FhirSchemaError, message, Operations.RunJob), 1);
                    break;
                case ContainerRegistryTokenException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.AuthenticationError, message, Operations.RunJob), 1);
                    break;
                case DataSerializationException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.DataProcessError, message, Operations.RunJob), 1);
                    break;
                case ContainerRegistryFilterException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.ReadFilterError, message, Operations.RunJob), 1);
                    break;
                case SynapsePipelineExternalException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.JobExecutionError, message, Operations.RunJob), 1);
                    break;
                default:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(false, ErrorType.JobExecutionError, message, Operations.RunJob), 1);
                    break;
            }
        }
    }
}
