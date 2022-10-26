// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions
{
    public class HealthCheckErrorProcessor
    {
        private IMetricsLogger _metricsLogger;

        public HealthCheckErrorProcessor(IMetricsLogger metricsLogger)
        {
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
        }

        public void Process(Exception ex, string message = "")
        {
            message += ex.Message;
            _metricsLogger.LogTotalErrorsMetrics(ErrorType.HealthCheckError, message, Operations.HealthCheck);
        }
    }
}
