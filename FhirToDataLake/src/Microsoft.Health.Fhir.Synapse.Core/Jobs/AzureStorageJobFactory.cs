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
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
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
    public class AzureStorageJobFactory : IJobFactory
    {
        private readonly IQueueClient _queueClient;
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly IGroupMemberExtractor _groupMemberExtractor;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly IFhirSchemaManager<FhirParquetSchemaNode> _fhirSchemaManager;
        private readonly IFilterManager _filterManager;
        private readonly IMetadataStore _metadataStore;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly int _maxJobCountInRunningPool;
        private readonly ILogger<AzureStorageJobFactory> _logger;
        private readonly IMetricsLogger _metricsLogger;
        private readonly IJobExecutionErrorProcessor _jobExecutionErrorProcessor;

        public AzureStorageJobFactory(
            IQueueClient queueClient,
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            IGroupMemberExtractor groupMemberExtractor,
            IColumnDataProcessor parquetDataProcessor,
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            IFilterManager filterManager,
            IMetadataStore metadataStore,
            IOptions<JobConfiguration> jobConfiguration,
            IJobExecutionErrorProcessor jobExecutionErrorProcessor,
            IMetricsLogger metricsLogger,
            IDiagnosticLogger diagnosticLogger,
            ILoggerFactory loggerFactory)
        {
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _groupMemberExtractor = EnsureArg.IsNotNull(groupMemberExtractor, nameof(groupMemberExtractor));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _fhirSchemaManager = EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
            _filterManager = EnsureArg.IsNotNull(filterManager, nameof(filterManager));
            _metadataStore = EnsureArg.IsNotNull(metadataStore, nameof(metadataStore));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _jobExecutionErrorProcessor = EnsureArg.IsNotNull(jobExecutionErrorProcessor, nameof(jobExecutionErrorProcessor));

            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            _maxJobCountInRunningPool = jobConfiguration.Value.MaxQueuedJobCountPerOrchestration;

            _loggerFactory = EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));
            _logger = _loggerFactory.CreateLogger<AzureStorageJobFactory>();
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
        }

        public IJob Create(JobInfo jobInfo)
        {
            EnsureArg.IsNotNull(jobInfo, nameof(jobInfo));

            if (_metadataStore.IsInitialized())
            {
                var taskFactoryFuncs =
                    new Func<JobInfo, IJob>[] { CreateProcessingTask, CreateOrchestratorTask };

                foreach (var factoryFunc in taskFactoryFuncs)
                {
                    var job = factoryFunc(jobInfo);
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

            _logger.LogInformation("Metadata store isn't initialized yet.");
            return null;
        }

        private IJob CreateOrchestratorTask(JobInfo jobInfo)
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobInputData>(jobInfo.Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    var currentResult = string.IsNullOrWhiteSpace(jobInfo.Result)
                        ? new FhirToDataLakeOrchestratorJobResult()
                        : JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobResult>(jobInfo.Result);

                    return new FhirToDataLakeOrchestratorJob(
                        jobInfo,
                        inputData,
                        currentResult,
                        _dataClient,
                        _dataWriter,
                        _queueClient,
                        _groupMemberExtractor,
                        _filterManager,
                        _metadataStore,
                        _maxJobCountInRunningPool,
                        _metricsLogger,
                        _jobExecutionErrorProcessor,
                        _diagnosticLogger,
                        _loggerFactory.CreateLogger<FhirToDataLakeOrchestratorJob>());
                }
            }
            catch (Exception e)
            {
                _metricsLogger.LogTotalErrorsMetrics(ErrorType.CreateJobError, $"Failed to create orchestrator job. Reason: {e.Message}", Operations.CreateJob);
                _logger.LogInformation(e, "Failed to create orchestrator job.");
                return null;
            }

            return null;
        }

        private IJob CreateProcessingTask(JobInfo jobInfo)
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobInputData>(jobInfo.Definition);
                if (inputData is { JobType: JobType.Processing })
                {
                    return new FhirToDataLakeProcessingJob(
                        jobInfo.Id,
                        inputData,
                        _dataClient,
                        _dataWriter,
                        _parquetDataProcessor,
                        _fhirSchemaManager,
                        _groupMemberExtractor,
                        _filterManager,
                        _jobExecutionErrorProcessor,
                        _diagnosticLogger,
                        _loggerFactory.CreateLogger<FhirToDataLakeProcessingJob>());
                }
            }
            catch (Exception e)
            {
                _metricsLogger.LogTotalErrorsMetrics(ErrorType.CreateJobError, $"Failed to create processing job. Reason: {e.Message}", Operations.CreateJob);
                _logger.LogInformation(e, "Failed to create processing job.");
                return null;
            }

            return null;
        }
    }
}