// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class AzureStorageJobFactory : IJobFactory
    {
        private IQueueClient _queueClient;
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly ILoggerFactory _loggerFactory;

        public AzureStorageJobFactory(
            IQueueClient queueClient,
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            ILoggerFactory loggerFactory)
        {
            _queueClient = queueClient;
            _dataClient = dataClient;
            _dataWriter = dataWriter;
            _parquetDataProcessor = parquetDataProcessor;
            _loggerFactory = loggerFactory;
        }

        public IJob Create(JobInfo jobInfo)
        {
            if (TryCreateOrchestratorJob(jobInfo, out OrchestratorJob orchestratorJob))
            {
                return orchestratorJob;
            }
            else if (TryCreateProcessJob(jobInfo, out ProcessJob processJob))
            {
                return processJob;
            }

            return null;
        }

        private bool TryCreateOrchestratorJob(JobInfo jobInfo, out OrchestratorJob job)
        {
            var orchestratorJobInputData = JsonConvert.DeserializeObject<OrchestratorJobInputData>(jobInfo.Definition);
            if (orchestratorJobInputData.TypeId != (int)JobType.Orchestrator)
            {
                job = null;
                return false;
            }

            var orchestratorJobResult = new OrchestratorJobResult()
            {
                CreatedJobTimestamp = orchestratorJobInputData.DataStart,
            };

            if (!string.IsNullOrEmpty(jobInfo.Result))
            {
                orchestratorJobResult = JsonConvert.DeserializeObject<OrchestratorJobResult>(jobInfo.Result);
            }

            job = new OrchestratorJob(jobInfo, orchestratorJobInputData, orchestratorJobResult, _dataClient, _dataWriter, _queueClient);

            return true;
        }

        private bool TryCreateProcessJob(JobInfo jobInfo, out ProcessJob job)
        {
            var processJobInputData = JsonConvert.DeserializeObject<ProcessJobInputData>(jobInfo.Definition);
            if (processJobInputData.TypeId != (int)JobType.Process)
            {
                job = null;
                return false;
            }

            var processJobResult = new ProcessJobResult();
            if (!string.IsNullOrEmpty(jobInfo.Result))
            {
                processJobResult = JsonConvert.DeserializeObject<ProcessJobResult>(jobInfo.Result);
            }

            job = new ProcessJob(jobInfo, processJobInputData, processJobResult, _dataClient, _dataWriter, _parquetDataProcessor, _loggerFactory.CreateLogger<ProcessJob>());

            return true;
        }
    }
}
