// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    /// <summary>
    /// Factory to create different jobs.
    /// </summary>
    public class DicomAzureStorageJobFactory : IJobFactory
    {
        private readonly IQueueClient _queueClient;
        private readonly IDicomDataClient _dataClient;
        private readonly IDataWriter _dataWriter;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly IFhirSchemaManager<FhirParquetSchemaNode> _fhirSchemaManager;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly int _maxJobCountInRunningPool;
        private readonly ILogger<DicomAzureStorageJobFactory> _logger;
        private readonly IMetricsLogger _metricsLogger;

        public DicomAzureStorageJobFactory(
            IQueueClient queueClient,
            IDicomDataClient dataClient,
            IDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            IOptions<JobConfiguration> jobConfiguration,
            IMetricsLogger metricsLogger,
            IDiagnosticLogger diagnosticLogger,
            ILoggerFactory loggerFactory)
        {
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _fhirSchemaManager = EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));

            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            _maxJobCountInRunningPool = jobConfiguration.Value.MaxQueuedJobCountPerOrchestration;

            _loggerFactory = EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));
            _logger = _loggerFactory.CreateLogger<DicomAzureStorageJobFactory>();
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
        }

        public IJob Create(JobInfo jobInfo)
        {
            EnsureArg.IsNotNull(jobInfo, nameof(jobInfo));

            Func<JobInfo, IJob>[] taskFactoryFuncs =
                new Func<JobInfo, IJob>[] { CreateProcessingTask, CreateOrchestratorTask };

            foreach (Func<JobInfo, IJob> factoryFunc in taskFactoryFuncs)
            {
                IJob job = factoryFunc(jobInfo);
                if (job != null)
                {
                    return job;
                }
            }

            // job hosting didn't catch any exception thrown during creating job,
            // return null for failure case, and job hosting will skip it.
            _logger.LogInformation($"Failed to create job, unknown job definition. ID: {jobInfo?.Id ?? -1}");
            return null;
        }

        private IJob CreateOrchestratorTask(JobInfo jobInfo)
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobInputData>(jobInfo.Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    DicomToDataLakeOrchestratorJobResult currentResult = string.IsNullOrWhiteSpace(jobInfo.Result)
                        ? new DicomToDataLakeOrchestratorJobResult()
                        : JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobResult>(jobInfo.Result);

                    return new DicomToDataLakeOrchestratorJob(
                        jobInfo,
                        inputData,
                        currentResult,
                        _dataClient,
                        _dataWriter,
                        _queueClient,
                        _maxJobCountInRunningPool,
                        _metricsLogger,
                        _diagnosticLogger,
                        _loggerFactory.CreateLogger<DicomToDataLakeOrchestratorJob>());
                }
            }
            catch (Exception e)
            {
                _metricsLogger.LogTotalErrorsMetrics(e, $"Failed to create orchestrator job. Reason: {e.Message}", Operations.CreateJob);
                _logger.LogInformation(e, "Failed to create orchestrator job.");
                return null;
            }

            return null;
        }

        private IJob CreateProcessingTask(JobInfo jobInfo)
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<DicomToDataLakeProcessingJobInputData>(jobInfo.Definition);
                if (inputData is { JobType: JobType.Processing })
                {
                    return new DicomToDataLakeProcessingJob(
                        jobInfo.Id,
                        inputData,
                        _dataClient,
                        _dataWriter,
                        _parquetDataProcessor,
                        _fhirSchemaManager,
                        _metricsLogger,
                        _diagnosticLogger,
                        _loggerFactory.CreateLogger<DicomToDataLakeProcessingJob>());
                }
            }
            catch (Exception e)
            {
                _metricsLogger.LogTotalErrorsMetrics(e, $"Failed to create processing job. Reason: {e.Message}", Operations.CreateJob);
                _logger.LogInformation(e, "Failed to create processing job.");
                return null;
            }

            return null;
        }
    }
}