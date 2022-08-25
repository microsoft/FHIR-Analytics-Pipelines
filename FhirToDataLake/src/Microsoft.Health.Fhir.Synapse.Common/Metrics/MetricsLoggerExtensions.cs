// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public static class MetricsLoggerExtensions
    {
        public static void LogTotalErrorMetrics(this IMetricsLogger metricsLogger, double value)
        {
            metricsLogger.LogMetrics(new TotalErrorsMetrics(), value);
        }
    }
}
