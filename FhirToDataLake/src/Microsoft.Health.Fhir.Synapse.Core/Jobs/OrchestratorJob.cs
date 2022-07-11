// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class OrchestratorJob : IJob
    {
        private readonly OrchestratorJobInputData _inputData;
        private readonly OrchestratorJobResult _result;
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly IQueueClient _queueClient;
        private readonly JobInfo _jobInfo;

        private const int _incrementalOrchestrationIntervalInSeconds = 60;
        private const int _initialOrchestrationIntervalInSeconds = 1800;
        private const int ConcurrencyCount = 20;

        public OrchestratorJob(
            JobInfo jobInfo,
            OrchestratorJobInputData inputData,
            OrchestratorJobResult result,
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            IQueueClient queueClient)
        {
            _jobInfo = jobInfo;
            _inputData = inputData;
            _result = result;
            _dataClient = dataClient;
            _dataWriter = dataWriter;
            _queueClient = queueClient;
        }

        public async Task<string> ExecuteAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _jobInfo.StartDate = DateTime.UtcNow;
            TimeSpan interval = TimeSpan.FromSeconds(_incrementalOrchestrationIntervalInSeconds);
            if (_inputData.DataStart.AddMinutes(60) < _inputData.DataEnd)
            {
                interval = TimeSpan.FromSeconds(_initialOrchestrationIntervalInSeconds);
            }

            while (_result.CreatedJobTimestamp < _inputData.DataEnd)
            {
                while (_result.RunningJobIds.Count >= ConcurrencyCount)
                {
                    await WaitFirstJobComplete(progress, cancellationToken);
                }

                var nextResourceTimestamp = await GetNextTimestamp(_result.CreatedJobTimestamp, _inputData.DataEnd);
                if (nextResourceTimestamp == default)
                {
                    _result.CreatedJobTimestamp = _inputData.DataEnd;
                    break;
                }

                DateTimeOffset nextJobEnd = nextResourceTimestamp + interval;
                if (nextJobEnd > _inputData.DataEnd)
                {
                    nextJobEnd = _inputData.DataEnd;
                }

                var newJobId = await CreateNewPocessJob(nextResourceTimestamp, nextJobEnd, cancellationToken);
                _result.RunningJobIds.Add(newJobId);
                _result.CreatedJobCount += 1;
                _result.CreatedJobTimestamp = nextJobEnd;
                progress.Report(JsonConvert.SerializeObject(_result));
            }

            while (_result.RunningJobIds.Count > 0)
            {
                await WaitFirstJobComplete(progress, cancellationToken);
            }

            _jobInfo.EndDate = DateTime.UtcNow;
            return JsonConvert.SerializeObject(_result);
        }

        private async Task<DateTimeOffset> GetNextTimestamp(DateTimeOffset start, DateTimeOffset end)
        {
            var searchParameters = new FhirSearchParameters(_inputData.ResourceTypes, start, _inputData.DataEnd, null);
            var fhirBundleResult = await _dataClient.SearchAsync(searchParameters);
            JObject fhirBundleObject;
            try
            {
                fhirBundleObject = JObject.Parse(fhirBundleResult);
            }
            catch (Exception exception)
            {
                throw new FhirDataParseExeption($"Failed to parse fhir search result", exception);
            }

            var fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject);
            if (!fhirResources.Any())
            {
                return default;
            }

            return fhirResources.First()?.GetLastUpdated() ?? default;
        }

        private async Task WaitFirstJobComplete(IProgress<string> progress, CancellationToken cancellationToken)
        {
            if (!_result.RunningJobIds.Any())
            {
                return;
            }

            var firstJobId = _result.RunningJobIds.Min();
            var latestJob = await _queueClient.GetJobByIdAsync(_jobInfo.QueueType, firstJobId, false, cancellationToken);
            if (latestJob.Status != JobStatus.Created && latestJob.Status != JobStatus.Running)
            {
                // Handle cancel/failed
                await CommitJobData(firstJobId, cancellationToken);
                if (latestJob.Result != null)
                {
                    var processResult = JsonConvert.DeserializeObject<ProcessJobResult>(latestJob.Result);
                    _result.SuccessfulResourceCount += processResult.SuccessfulResourceCount;
                    _result.FailedResourceCount += processResult.FailedResourceCount;
                }

                _result.RunningJobIds.Remove(firstJobId);
                progress.Report(JsonConvert.SerializeObject(_result));
            }
            else
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
            }
        }

        private async Task CommitJobData(long jobId, CancellationToken cancellationToken)
        {
            await _dataWriter.CommitJobDataAsync(jobId, cancellationToken);
        }

        private async Task<long> CreateNewPocessJob(DateTimeOffset subDataStart, DateTimeOffset subDataEnd, CancellationToken cancellationToken)
        {
            ProcessJobInputData input = new ProcessJobInputData
            {
                FhirServerUrl = _inputData.FhirServerUrl,
                DataLakeUrl = _inputData.DataLakeUrl,
                DataStart = subDataStart,
                DataEnd = subDataEnd,
                ContainerName = _inputData.ContainerName,
                ResourceTypes = _inputData.ResourceTypes,
                TypeId = (int)JobType.Process,
            };

            string[] jobDefinitions = new string[] { JsonConvert.SerializeObject(input) };
            var jobInfos = await _queueClient.EnqueueAsync(_jobInfo.QueueType, jobDefinitions, _jobInfo.GroupId, false, false, cancellationToken);
            return jobInfos.First().Id;
        }
    }
}
