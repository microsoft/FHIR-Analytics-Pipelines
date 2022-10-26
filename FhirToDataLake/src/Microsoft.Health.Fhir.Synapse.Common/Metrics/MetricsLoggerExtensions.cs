// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public static class MetricsLoggerExtensions
    {
        public static void LogTotalErrorsMetrics(this IMetricsLogger metricsLogger, bool isDiagnostic, string errorType, string reason, string operation)
        {
            metricsLogger.LogMetrics(new TotalErrorsMetric(isDiagnostic, errorType, reason, operation), 1);
        }

        public static void LogSuccessfulResourceCountMetric(this IMetricsLogger metricsLogger, double value)
        {
            metricsLogger.LogMetrics(new SuccessfulResourceCountMetric(), value);
        }

        public static void LogSuccessfulDataSizeMetric(this IMetricsLogger metricsLogger, double value)
        {
            metricsLogger.LogMetrics(new SuccessfulDataSizeMetric(), value);
        }

        public static void LogProcessLatencyMetric(this IMetricsLogger metricsLogger, double value)
        {
            metricsLogger.LogMetrics(new ProcessLatencyMetric(), value);
        }

        public static void LogHealthStatusMetric(this IMetricsLogger metricsLogger, double value)
        {
            metricsLogger.LogMetrics(new HealthStatusMetric(), value);
        }
    }
}