// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests
{
    public class MockHealthCheckMetricsLogger : IMetricsLogger
    {
        public MockHealthCheckMetricsLogger(ILogger<MockHealthCheckMetricsLogger> logger)
        {
        }

        public Dictionary<string, double> HealthStatus { get; set; } = new Dictionary<string, double>();

        public string ErrorOperationType { get; set; }

        public void LogMetrics(Metrics metrics, double metricsValue)
        {
            if (metrics is TotalErrorsMetric)
            {
                ErrorOperationType = ((TotalErrorsMetric)metrics).Dimensions["Operation"].ToString();
                return;
            }

            var component = ((HealthStatusMetric)metrics).Dimensions["Component"].ToString();
            HealthStatus[component] = metricsValue;
        }
    }
}
