﻿// -------------------------------------------------------------------------------------------------
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
    public class FhirAzureStorageJobFactory : IJobFactory
    {
        private readonly IQueueClient _queueClient;
        private readonly IApiDataClient _dataClient;
        private readonly IDataWriter _dataWriter;
        private readonly IGroupMemberExtractor _groupMemberExtractor;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly ISchemaManager<ParquetSchemaNode> _schemaManager;
        private readonly IFilterManager _filterManager;
        private readonly IMetadataStore _metadataStore;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly int _maxJobCountInRunningPool;
        private readonly ILogger<FhirAzureStorageJobFactory> _logger;
        private readonly IMetricsLogger _metricsLogger;

        public FhirAzureStorageJobFactory(
            IQueueClient queueClient,
            IApiDataClient dataClient,
            IDataWriter dataWriter,
            IGroupMemberExtractor groupMemberExtractor,
            IColumnDataProcessor parquetDataProcessor,
            ISchemaManager<ParquetSchemaNode> schemaManager,
            IFilterManager filterManager,
            IMetadataStore metadataStore,
            IOptions<JobConfiguration> jobConfiguration,
            IMetricsLogger metricsLogger,
            IDiagnosticLogger diagnosticLogger,
            ILoggerFactory loggerFactory)
        {
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _groupMemberExtractor = EnsureArg.IsNotNull(groupMemberExtractor, nameof(groupMemberExtractor));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _schemaManager = EnsureArg.IsNotNull(schemaManager, nameof(schemaManager));
            _filterManager = EnsureArg.IsNotNull(filterManager, nameof(filterManager));
            _metadataStore = EnsureArg.IsNotNull(metadataStore, nameof(metadataStore));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));

            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            _maxJobCountInRunningPool = jobConfiguration.Value.MaxQueuedJobCountPerOrchestration;

            _loggerFactory = EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));
            _logger = _loggerFactory.CreateLogger<FhirAzureStorageJobFactory>();
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
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobInputData>(jobInfo.Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    // return null if the job version is unsupported
                    if (FhirJobVersionManager.SupportedJobVersion.Contains(inputData.JobVersion))
                    {
                        FhirToDataLakeOrchestratorJobResult currentResult = string.IsNullOrWhiteSpace(jobInfo.Result)
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
                            _diagnosticLogger,
                            _loggerFactory.CreateLogger<FhirToDataLakeOrchestratorJob>());
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
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobInputData>(jobInfo.Definition);
                if (inputData is { JobType: JobType.Processing })
                {
                    // return null if the job version is unsupported
                    if (FhirJobVersionManager.SupportedJobVersion.Contains(inputData.JobVersion))
                    {
                        return new FhirToDataLakeProcessingJob(
                        jobInfo.Id,
                        inputData,
                        _dataClient,
                        _dataWriter,
                        _parquetDataProcessor,
                        _schemaManager,
                        _groupMemberExtractor,
                        _filterManager,
                        _metricsLogger,
                        _diagnosticLogger,
                        _loggerFactory.CreateLogger<FhirToDataLakeProcessingJob>());
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