// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure.Data.Tables;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.JobManagement;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
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
        private readonly JobSchedulerConfiguration _schedulerConfiguration;
        private readonly FilterConfiguration _filterConfiguration;
        private readonly AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo> _queueClient;
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly ITypeFilterParser _typeFilterParser;
        private readonly IGroupMemberExtractor _groupMemberExtractor;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly TableClient _metaDataTableClient;
        private readonly IFhirSchemaManager<FhirParquetSchemaNode> _fhirSchemaManager;
        private readonly ILoggerFactory _loggerFactory;

        public AzureStorageJobFactory(
            AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo> queueClient,
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            ITypeFilterParser typeFilterParser,
            IGroupMemberExtractor groupMemberExtractor,
            IColumnDataProcessor parquetDataProcessor,
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            IAzureTableClientFactory azureTableClientFactory,
            IOptions<JobSchedulerConfiguration> schedulerConfiguration,
            IOptions<FilterConfiguration> filterConfiguration,
            ILoggerFactory loggerFactory)
        {
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _typeFilterParser = EnsureArg.IsNotNull(typeFilterParser, nameof(typeFilterParser));
            _groupMemberExtractor = EnsureArg.IsNotNull(groupMemberExtractor, nameof(groupMemberExtractor));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _fhirSchemaManager = EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));

            EnsureArg.IsNotNull(azureTableClientFactory, nameof(azureTableClientFactory));

            _metaDataTableClient = azureTableClientFactory.Create();
            _metaDataTableClient.CreateIfNotExists();

            EnsureArg.IsNotNull(schedulerConfiguration, nameof(schedulerConfiguration));
            _schedulerConfiguration = schedulerConfiguration.Value;

            EnsureArg.IsNotNull(filterConfiguration, nameof(filterConfiguration));
            _filterConfiguration = filterConfiguration.Value;

            _loggerFactory = EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));
        }

        public IJob Create(JobInfo jobInfo)
        {
            EnsureArg.IsNotNull(jobInfo, nameof(jobInfo));

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

            throw new NotSupportedException($"Unknown job definition. ID: {jobInfo?.Id ?? -1}");
        }

        private IJob CreateOrchestratorTask(JobInfo jobInfo)
        {
            // TODO: what if deserialize exception
            var inputData = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobInputData>(jobInfo.Definition);
            if (inputData is { JobType: JobType.Orchestrator })
            {
                var currentResult = string.IsNullOrEmpty(jobInfo.Result)
                    ? new FhirToDataLakeOrchestratorJobResult()
                    : JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobResult>(jobInfo.Result);

                return new FhirToDataLakeOrchestratorJob(
                    jobInfo,
                    inputData,
                    currentResult,
                    _dataClient,
                    _dataWriter,
                    _queueClient,
                    _typeFilterParser,
                    _groupMemberExtractor,
                    _metaDataTableClient,
                    _schedulerConfiguration,
                    _filterConfiguration,
                    _loggerFactory.CreateLogger<FhirToDataLakeOrchestratorJob>());
            }

            return null;
        }

        private IJob CreateProcessingTask(JobInfo jobInfo)
        {
            var inputData = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobInputData>(jobInfo.Definition);
            if (inputData is { JobType: JobType.Processing })
            {
                return new FhirToDataLakeProcessingJob(
                    jobInfo,
                    inputData,
                    _dataClient,
                    _dataWriter,
                    _parquetDataProcessor,
                    _fhirSchemaManager,
                    _loggerFactory.CreateLogger<FhirToDataLakeProcessingJob>());
            }

            return null;
        }
    }
}
