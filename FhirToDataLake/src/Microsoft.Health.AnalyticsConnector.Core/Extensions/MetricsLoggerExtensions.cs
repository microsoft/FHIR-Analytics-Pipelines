// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.DataClient.Exceptions;
using Microsoft.Health.AnalyticsConnector.DataWriter.Exceptions;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Exceptions;

namespace Microsoft.Health.AnalyticsConnector.Core.Extensions
{
    public static class MetricsLoggerExtensions
    {
        public static void LogTotalErrorsMetrics(this IMetricsLogger metricsLogger, Exception ex, string message, string operationType)
        {
            switch (operationType)
            {
                case nameof(JobOperations.RunSchedulerService):
                    metricsLogger.LogMetrics(new TotalErrorsMetric(false, ErrorType.SchedulerServiceError, message, JobOperations.RunSchedulerService), 1);
                    break;
                case nameof(JobOperations.HealthCheck):
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.HealthCheckError, message, JobOperations.HealthCheck), 1);
                    break;
                case nameof(JobOperations.CreateJob):
                    metricsLogger.LogMetrics(new TotalErrorsMetric(false, ErrorType.CreateJobError, message, JobOperations.CreateJob), 1);
                    break;
                case nameof(JobOperations.RunJob):
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
                case ApiSearchException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.ReadFhirDataError, message, JobOperations.RunJob), 1);
                    break;
                case AzureBlobOperationFailedException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.WriteToDatalakeError, message, JobOperations.RunJob), 1);
                    break;
                case FhirSchemaException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.FhirSchemaError, message, JobOperations.RunJob), 1);
                    break;
                case ContainerRegistryTokenException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.AuthenticationError, message, JobOperations.RunJob), 1);
                    break;
                case DataSerializationException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.DataProcessError, message, JobOperations.RunJob), 1);
                    break;
                case ContainerRegistryFilterException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.ReadFilterError, message, JobOperations.RunJob), 1);
                    break;
                case SynapsePipelineExternalException:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(true, ErrorType.JobExecutionError, message, JobOperations.RunJob), 1);
                    break;
                default:
                    metricsLogger.LogMetrics(new TotalErrorsMetric(false, ErrorType.JobExecutionError, message, JobOperations.RunJob), 1);
                    break;
            }
        }
    }
}
