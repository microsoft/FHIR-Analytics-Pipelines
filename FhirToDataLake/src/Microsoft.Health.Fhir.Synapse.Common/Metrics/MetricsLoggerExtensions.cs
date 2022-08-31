// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public static class MetricsLoggerExtensions
    {
        public static void LogTotalErrorsMetrics(this IMetricsLogger metricsLogger, double value, string errorType, string reason, string operation)
        {
            metricsLogger.LogMetrics(new TotalErrorsMetric(errorType, reason, operation), value);
        }

        public static void LogSuccessfulResourceCountMetrics(this IMetricsLogger metricsLogger, double value)
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