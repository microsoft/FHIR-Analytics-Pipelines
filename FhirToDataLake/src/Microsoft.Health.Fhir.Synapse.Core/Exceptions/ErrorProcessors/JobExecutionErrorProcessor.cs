// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions.ErrorProcessors
{
    public class JobExecutionErrorProcessor : IJobExecutionErrorProcessor
    {
        private IMetricsLogger _metricsLogger;

        public JobExecutionErrorProcessor(IMetricsLogger metricsLogger)
        {
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
        }

        public void Process(Exception ex, string message = "")
        {
            message = string.Format("{0} Reason:{1}", message, ex.Message);
            switch (ex)
            {
                case FhirSearchException:
                    _metricsLogger.LogTotalErrorsMetrics(true, ErrorType.ReadFhirDataError, message, Operations.RunJob);
                    break;
                case AzureBlobOperationFailedException:
                    _metricsLogger.LogTotalErrorsMetrics(true, ErrorType.WriteToDatalakeError, message, Operations.RunJob);
                    break;
                case FhirSchemaException:
                    _metricsLogger.LogTotalErrorsMetrics(true, ErrorType.FhirSchemaError, message, Operations.RunJob);
                    break;
                case ContainerRegistryTokenException:
                    _metricsLogger.LogTotalErrorsMetrics(true, ErrorType.AuthenticationError, message, Operations.RunJob);
                    break;
                case DataSerializationException:
                    _metricsLogger.LogTotalErrorsMetrics(true, ErrorType.DataProcessError, message, Operations.RunJob);
                    break;
                case ContainerRegistryFilterException:
                    _metricsLogger.LogTotalErrorsMetrics(true, ErrorType.ReadFilterError, message, Operations.RunJob);
                    break;
                case SynapsePipelineExternalException:
                    _metricsLogger.LogTotalErrorsMetrics(true, ErrorType.JobExecutionError, message, Operations.RunJob);
                    break;
                default:
                    _metricsLogger.LogTotalErrorsMetrics(false, ErrorType.JobExecutionError, message, Operations.RunJob);
                    break;
            }
        }
    }
}
