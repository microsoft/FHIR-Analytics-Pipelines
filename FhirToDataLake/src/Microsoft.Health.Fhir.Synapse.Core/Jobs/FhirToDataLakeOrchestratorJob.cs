// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.JobManagement;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using JobStatus = Microsoft.Health.JobManagement.JobStatus;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class FhirToDataLakeOrchestratorJob : IJob
    {
        private readonly FhirToDataLakeOrchestratorJobInputData _inputData;
        private readonly FhirToDataLakeOrchestratorJobResult _result;
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo> _queueClient;
        private readonly ITypeFilterParser _typeFilterParser;
        private readonly IGroupMemberExtractor _groupMemberExtractor;
        private readonly TableClient _metaDataTableClient;

        private readonly JobSchedulerConfiguration _schedulerConfiguration;
        private readonly FilterConfiguration _filterConfiguration;

        private const int _incrementalOrchestrationIntervalInSeconds = 60;
        private const int _initialOrchestrationIntervalInSeconds = 1800;

        private readonly JobInfo _jobInfo;

        private ILogger<FhirToDataLakeOrchestratorJob> _logger;

        public FhirToDataLakeOrchestratorJob(
            JobInfo jobInfo,
            FhirToDataLakeOrchestratorJobInputData inputData,
            FhirToDataLakeOrchestratorJobResult result,
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo> queueClient,
            ITypeFilterParser typeFilterParser,
            IGroupMemberExtractor groupMemberExtractor,
            TableClient metaDataTableClient,
            JobSchedulerConfiguration schedulerConfiguration,
            FilterConfiguration filterConfiguration,
            ILogger<FhirToDataLakeOrchestratorJob> logger)
        {
            _jobInfo = EnsureArg.IsNotNull(jobInfo, nameof(jobInfo));
            _inputData = EnsureArg.IsNotNull(inputData, nameof(inputData));
            _result = EnsureArg.IsNotNull(result, nameof(result));
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            _typeFilterParser = EnsureArg.IsNotNull(typeFilterParser, nameof(typeFilterParser));
            _groupMemberExtractor = EnsureArg.IsNotNull(groupMemberExtractor, nameof(groupMemberExtractor));
            _metaDataTableClient = EnsureArg.IsNotNull(metaDataTableClient, nameof(metaDataTableClient));
            _schedulerConfiguration = EnsureArg.IsNotNull(schedulerConfiguration, nameof(schedulerConfiguration));
            _filterConfiguration = EnsureArg.IsNotNull(filterConfiguration, nameof(filterConfiguration));

            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public async Task<string> ExecuteAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start executing FhirToDataLake orchestrator job {_jobInfo.GroupId}");

            _jobInfo.StartDate = DateTime.UtcNow;

            try
            {
                var typeFilters = _typeFilterParser.CreateTypeFilters(
                _filterConfiguration.FilterScope,
                _filterConfiguration.RequiredTypes,
                _filterConfiguration.TypeFilters).ToList();

                // extract patient ids from group
                if (_filterConfiguration.FilterScope == FilterScope.Group)
                {
                    _logger.LogInformation("Start extracting patients from group '{groupId}'.", _filterConfiguration.GroupId);

                    // For now, the queryParameters is always null.
                    // This parameter will be used when we enable filter groups in the future.
                    var patientIds = await _groupMemberExtractor.GetGroupPatientsAsync(
                        _filterConfiguration.GroupId,
                        null,
                        _inputData.DataEndTime,
                        cancellationToken);

                    var processedPatientVersions = await GetPatientVersions(patientIds, cancellationToken);

                    // set the version id for processed patient
                    // the processed patients is empty at the beginning , and will be updated when completing a successful job.
                    var toBeProcessedPatients = patientIds.Select(patientId =>
                        new PatientWrapper(
                            patientId,
                            processedPatientVersions.ContainsKey(patientId) ? processedPatientVersions[patientId] : 0)).ToList();

                    _logger.LogInformation(
                        "Extract {patientCount} patients from group '{groupId}', including {newPatientCount} new patients.",
                        patientIds.Count,
                        _filterConfiguration.GroupId,
                        toBeProcessedPatients.Where(p => p.VersionId == 0).ToList().Count);
                }

                TimeSpan interval = TimeSpan.FromSeconds(_incrementalOrchestrationIntervalInSeconds);
                /*
                if (_inputData.DataStartTime == null | _inputData.DataStartTime.AddMinutes(60) < _inputData.DataEndTime)
                {
                    interval = TimeSpan.FromSeconds(_initialOrchestrationIntervalInSeconds);
                }
                */
                while (true)
                {
                    while (_result.RunningJobIds.Count > _schedulerConfiguration.MaxConcurrencyCount)
                    {
                        await WaitFirstJobComplete(progress, cancellationToken);
                    }

                    FhirToDataLakeProcessingJobInputData input = null;

                    switch (_filterConfiguration.FilterScope)
                    {
                        case FilterScope.System:
                            if (_result.CreatedJobTimestamp < _inputData.DataEndTime)
                            {
                                var nextResourceTimestamp =
                                    await GetNextTimestamp(_result.CreatedJobTimestamp, _inputData.DataEndTime);
                                if (nextResourceTimestamp == default)
                                {
                                    _result.CreatedJobTimestamp = _inputData.DataEndTime;
                                    break;
                                }

                                DateTimeOffset nextJobEnd = nextResourceTimestamp + interval;
                                if (nextJobEnd > _inputData.DataEndTime)
                                {
                                    nextJobEnd = _inputData.DataEndTime;
                                }

                                input = new FhirToDataLakeProcessingJobInputData
                                {
                                    JobType = JobType.Processing,
                                    ProcessingJobSequenceId = _result.NextTaskIndex,
                                    Since = _inputData.Since,
                                    DataStartTime = nextResourceTimestamp,
                                    DataEndTime = nextJobEnd,
                                    FilterScope = _inputData.FilterConfiguration.FilterScope,
                                    TypeFilters = typeFilters,
                                    InputMetadata = new BaseProcessingInputMetadata(),
                                };
                                _result.CreatedJobTimestamp = nextJobEnd;
                            }

                            break;
                        case FilterScope.Group:
                            if (_result.NextTaskIndex * JobConfigurationConstants.NumberOfPatientsPerTask <
                                _result.ToBeProcessedPatients.Count)
                            {
                                var selectedPatients = _result.ToBeProcessedPatients.Skip((int)_result.NextTaskIndex *
                                        JobConfigurationConstants.NumberOfPatientsPerTask)
                                    .Take(JobConfigurationConstants.NumberOfPatientsPerTask).ToList();
                                input = new FhirToDataLakeProcessingJobInputData
                                {
                                    JobType = JobType.Processing,
                                    ProcessingJobSequenceId = _result.NextTaskIndex,
                                    Since = _inputData.Since,
                                    DataStartTime = _inputData.DataStartTime,
                                    DataEndTime = _inputData.DataEndTime,
                                    FilterScope = _inputData.FilterConfiguration.FilterScope,
                                    TypeFilters = typeFilters,
                                    InputMetadata = new CompartmentProcessingInputMetadata { ToBeProcessedPatients = selectedPatients, },
                                };
                                _result.NextTaskIndex++;
                            }

                            break;
                        default:
                            // this case should not happen
                            throw new ArgumentOutOfRangeException(
                                $"The filterScope {_inputData.FilterConfiguration.FilterScope} isn't supported now.");
                    }

                    if (input == null)
                    {
                        break;
                    }

                    string[] jobDefinitions = new string[] { JsonConvert.SerializeObject(input) };
                    var jobInfos = await _queueClient.EnqueueAsync(_jobInfo.QueueType, jobDefinitions, _jobInfo.GroupId,
                        false, false, cancellationToken);
                    var newJobId = jobInfos.First().Id;

                    _result.RunningJobIds.Add(newJobId);
                    progress.Report(JsonConvert.SerializeObject(_result));
                }

                while (_result.RunningJobIds.Count > 0)
                {
                    await WaitFirstJobComplete(progress, cancellationToken);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            _jobInfo.EndDate = DateTime.UtcNow;
            return JsonConvert.SerializeObject(_result);
        }

        private async Task<DateTimeOffset> GetNextTimestamp(DateTimeOffset start, DateTimeOffset end)
        {
            var searchParameters =
                new FhirSearchParameters(_inputData.ResourceTypes, start, _inputData.DataEndTime, null);
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
            var latestJob =
                await _queueClient.GetJobByIdAsync(_jobInfo.QueueType, firstJobId, false, cancellationToken);
            if (latestJob.Status != JobStatus.Created && latestJob.Status != JobStatus.Running)
            {
                // Handle cancel/failed
                if (latestJob.Status == JobStatus.Completed)
                {
                    await CommitJobData(firstJobId, cancellationToken);
                    if (latestJob.Result != null)
                    {
                        var processingJobResult =
                            JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobResult>(latestJob.Result);
                        _result.TotalResourceCounts =
                            _result.TotalResourceCounts.ConcatDictionaryCount(processingJobResult.SearchCount);
                        _result.ProcessedResourceCounts =
                            _result.ProcessedResourceCounts.ConcatDictionaryCount(processingJobResult.ProcessedCount);
                        _result.SkippedResourceCounts =
                            _result.SkippedResourceCounts.ConcatDictionaryCount(processingJobResult.SkippedCount);

                        if (_inputData.FilterConfiguration.FilterScope == FilterScope.Group)
                        {
                            foreach (var (patientId, versionId) in ((CompartmentResultMetadata)processingJobResult.ResultMetadata).ProcessedPatientVersions)
                            {
                                ((CompartmentResultMetadata)_result.ResultMetadata).ProcessedPatientVersions[patientId] = versionId;
                            }
                        }
                    }
                }
                else if (latestJob.Status == JobStatus.Failed)
                {
                    // TODO: 
                }
                else if (latestJob.Status == JobStatus.Cancelled)
                {
                    throw new OperationCanceledException("Import operation cancelled by customer.");
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

        private async Task<long> CreateNewProcessingJob(DateTimeOffset subDataStart, DateTimeOffset subDataEnd,
            CancellationToken cancellationToken)
        {
            FhirToDataLakeProcessingJobInputData input = new FhirToDataLakeProcessingJobInputData
            {

                DataStartTime = subDataStart,
                DataEndTime = subDataEnd,
                ContainerName = _inputData.ContainerName,
                ResourceTypes = _inputData.ResourceTypes,
                JobType = JobType.Processing,
                InputMetadata = CreateProcessingInputMetadata(),

            };

            string[] jobDefinitions = new string[] {JsonConvert.SerializeObject(input)};
            var jobInfos = await _queueClient.EnqueueAsync(_jobInfo.QueueType, jobDefinitions, _jobInfo.GroupId, false,
                false, cancellationToken);
            return jobInfos.First().Id;
        }

        private async Task<Dictionary<string, long>> GetPatientVersions(
            IEnumerable<string> patientIds,
            CancellationToken cancellationToken)
        {
            var pk = JobKeyProvider.CompartmentPartitionKey(_jobInfo.QueueType);
            var patientHashToId = patientIds.ToDictionary(patientId => patientId.ComputeHash(), patientId => patientId);

            var jobEntityQueryResult = _metaDataTableClient.QueryAsync<CompartmentInfoEntity>(
                filter: TransactionGetByKeys(pk, patientHashToId.Values.ToList()),
                cancellationToken: cancellationToken);

            var patientVersions = new Dictionary<string, long>();

            await foreach (var pageResult in jobEntityQueryResult.AsPages().WithCancellation(cancellationToken))
            {
                foreach (var entity in pageResult.Values)
                {
                    patientVersions[patientHashToId[entity.RowKey]] = entity.VersionId;
                }
            }

            return patientVersions;
        }

        private static string TransactionGetByKeys(string pk, List<string> rowKeys) =>
            $"PartitionKey eq '{pk}' and ({string.Join(" or ", rowKeys.Select(rowKey => $"RowKey eq '{rowKey}'"))})";

        private BaseProcessingInputMetadata CreateProcessingInputMetadata() =>
            _inputData.FilterConfiguration.FilterScope == FilterScope.System
                ? new BaseProcessingInputMetadata()
                : new CompartmentProcessingInputMetadata();
    }
}
