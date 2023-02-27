// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;
using Microsoft.Health.AnalyticsConnector.Core.DataProcessor;
using Microsoft.Health.AnalyticsConnector.Core.Extensions;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataWriter;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    /// <summary>
    /// Factory to create different jobs.
    /// </summary>
    public class FhirDataLakeToDataLakeAzureStorageJobFactory : IJobFactory
    {
        private readonly IQueueClient _queueClient;
        private readonly IDataLakeClient _dataLakeClient;
        private readonly IDataWriter _dataWriter;
        private readonly ICompletedBlobStore _completedBlobStore;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly ISchemaManager<ParquetSchemaNode> _schemaManager;
        private readonly IMetadataStore _metadataStore;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly int _maxJobCountInRunningPool;
        private readonly ILogger<FhirDataLakeToDataLakeAzureStorageJobFactory> _logger;
        private readonly IMetricsLogger _metricsLogger;

        public FhirDataLakeToDataLakeAzureStorageJobFactory(
            IQueueClient queueClient,
            IDataLakeClient dataLakeClient,
            IDataWriter dataWriter,
            ICompletedBlobStore completedBlobStore,
            IColumnDataProcessor parquetDataProcessor,
            ISchemaManager<ParquetSchemaNode> schemaManager,
            IMetadataStore metadataStore,
            IOptions<JobConfiguration> jobConfiguration,
            IMetricsLogger metricsLogger,
            IDiagnosticLogger diagnosticLogger,
            ILoggerFactory loggerFactory)
        {
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            _dataLakeClient = EnsureArg.IsNotNull(dataLakeClient, nameof(dataLakeClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _completedBlobStore = EnsureArg.IsNotNull(completedBlobStore, nameof(completedBlobStore));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _schemaManager = EnsureArg.IsNotNull(schemaManager, nameof(schemaManager));
            _metadataStore = EnsureArg.IsNotNull(metadataStore, nameof(metadataStore));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));

            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            _maxJobCountInRunningPool = jobConfiguration.Value.MaxQueuedJobCountPerOrchestration;

            _loggerFactory = EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));
            _logger = _loggerFactory.CreateLogger<FhirDataLakeToDataLakeAzureStorageJobFactory>();
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
        }

        public IJob Create(JobInfo jobInfo)
        {
            EnsureArg.IsNotNull(jobInfo, nameof(jobInfo));

            if (_metadataStore.IsInitialized())
            {
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

            _logger.LogInformation("Metadata store isn't initialized yet.");
            return null;
        }

        private IJob CreateOrchestratorTask(JobInfo jobInfo)
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirDataLakeToDataLakeOrchestratorJobInputData>(jobInfo.Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    // return null if the job version is unsupported
                    if (FhirDataLakeToDataLakeJobVersionManager.SupportedJobVersion.Contains(inputData.JobVersion))
                    {
                        FhirDataLakeToDataLakeOrchestratorJobResult currentResult = string.IsNullOrWhiteSpace(jobInfo.Result)
                            ? new FhirDataLakeToDataLakeOrchestratorJobResult()
                            : JsonConvert.DeserializeObject<FhirDataLakeToDataLakeOrchestratorJobResult>(jobInfo.Result);

                        return new FhirDataLakeToDataLakeOrchestratorJob(
                            jobInfo,
                            inputData,
                            currentResult,
                            _dataLakeClient,
                            _dataWriter,
                            _queueClient,
                            _completedBlobStore,
                            _maxJobCountInRunningPool,
                            _metricsLogger,
                            _diagnosticLogger,
                            _loggerFactory.CreateLogger<FhirDataLakeToDataLakeOrchestratorJob>());
                    }
                }
            }
            catch (Exception e)
            {
                _metricsLogger.LogTotalErrorsMetrics(e, $"Failed to create orchestrator job. Reason: {e.Message}", JobOperations.CreateJob);
                _logger.LogInformation(e, "Failed to create orchestrator job.");
                return null;
            }

            return null;
        }

        private IJob CreateProcessingTask(JobInfo jobInfo)
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirDataLakeToDataLakeProcessingJobInputData>(jobInfo.Definition);
                if (inputData is { JobType: JobType.Processing })
                {
                    // return null if the job version is unsupported
                    if (FhirDataLakeToDataLakeJobVersionManager.SupportedJobVersion.Contains(inputData.JobVersion))
                    {
                        FhirDataLakeToDataLakeProcessingJobResult currentResult = string.IsNullOrWhiteSpace(jobInfo.Result)
                            ? new FhirDataLakeToDataLakeProcessingJobResult()
                            : JsonConvert.DeserializeObject<FhirDataLakeToDataLakeProcessingJobResult>(jobInfo.Result);

                        return new FhirDataLakeToDataLakeProcessingJob(
                            jobInfo.Id,
                            inputData,
                            currentResult,
                            _dataLakeClient,
                            _dataWriter,
                            _parquetDataProcessor,
                            _schemaManager,
                            _metricsLogger,
                            _diagnosticLogger,
                            _loggerFactory.CreateLogger<FhirDataLakeToDataLakeProcessingJob>());
                    }
                }
            }
            catch (Exception e)
            {
                _metricsLogger.LogTotalErrorsMetrics(e, $"Failed to create processing job. Reason: {e.Message}", JobOperations.CreateJob);
                _logger.LogInformation(e, "Failed to create processing job.");
                return null;
            }

            return null;
        }
    }
}