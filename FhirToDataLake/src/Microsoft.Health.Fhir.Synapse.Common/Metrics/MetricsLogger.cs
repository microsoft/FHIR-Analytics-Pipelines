// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Linq;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public class MetricsLogger : IMetricsLogger
    {
        private readonly ILogger<MetricsLogger> _logger;

        public MetricsLogger(ILogger<MetricsLogger> logger)
        {
            _logger = logger;
        }

        public void LogMetrics(Metrics metrics, long metricsValue)
        {
            _logger.LogInformation(
                $"Log Metrics \r\n " +
                $"{metrics.Name} : Value \r\n " +
                $"Dimensions: \r\n" +
                $"{string.Join("\r\n", metrics.Dimensions.Select(x => string.Join(":", x.Key, x.Value.ToString())))}");
        }
    }
}
