// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.Jobs
{
    public class MockMetricsLogger : IMetricsLogger
    {
        public MockMetricsLogger(ILogger<MockMetricsLogger> logger)
        {
        }

        public Dictionary<string, double> MetricsDic { get; set; } = new Dictionary<string, double>();

        public string ErrorOperationType { get; set; }

        public void LogMetrics(Metrics metrics, double metricsValue)
        {
            if (metrics is TotalErrorsMetric)
            {
                ErrorOperationType = ((TotalErrorsMetric)metrics).Dimensions["Operation"].ToString();
            }

            if (MetricsDic.ContainsKey(metrics.Name))
            {
                MetricsDic[metrics.Name] += metricsValue;
            }
            else
            {
                MetricsDic[metrics.Name] = metricsValue;
            }
        }

        public void Clear()
        {
            MetricsDic.Clear();
        }
    }
}
