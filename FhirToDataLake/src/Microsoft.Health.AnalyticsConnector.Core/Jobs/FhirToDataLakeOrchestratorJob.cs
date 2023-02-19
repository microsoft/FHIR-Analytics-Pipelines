// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using EnsureThat;
using FellowOakDicom.Imaging.LUT;
using Hl7.Fhir.Utility;
using Hl7.FhirPath.Sprache;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;
using Microsoft.Health.AnalyticsConnector.Common.Models.FhirSearch;
using Microsoft.Health.AnalyticsConnector.Common.Models.Jobs;
using Microsoft.Health.AnalyticsConnector.Core.DataFilter;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.Core.Extensions;
using Microsoft.Health.AnalyticsConnector.Core.Fhir;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Fhir;
using Microsoft.Health.AnalyticsConnector.DataClient.Extensions;
using Microsoft.Health.AnalyticsConnector.DataClient.Models.FhirApiOption;
using Microsoft.Health.AnalyticsConnector.DataWriter;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public class FhirToDataLakeOrchestratorJob : IJob
    {
        private readonly JobInfo _jobInfo;
        private readonly FhirToDataLakeOrchestratorJobInputData _inputData;
        private readonly IApiDataClient _dataClient;
        private readonly IDataWriter _dataWriter;
        private readonly IQueueClient _queueClient;
        private readonly IGroupMemberExtractor _groupMemberExtractor;
        private readonly IMetadataStore _metadataStore;
        private readonly IFilterManager _filterManager;
        private readonly int _maxJobCountInRunningPool;
        private readonly ILogger<FhirToDataLakeOrchestratorJob> _logger;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly IMetricsLogger _metricsLogger;
        private FhirToDataLakeOrchestratorJobStatus _jobStatus;
        private OrchestratorJobStatusEntity _jobStatusEntity;
        private FhirToDataLakeProcessingJobSplitter _jobSplitter;
        private FhirToDataLakeOrchestratorJobResult _result;

        public FhirToDataLakeOrchestratorJob(
            JobInfo jobInfo,
            FhirToDataLakeOrchestratorJobInputData inputData,
            FhirToDataLakeOrchestratorJobResult result,
            FhirToDataLakeProcessingJobSplitter jobSplitter,
            IApiDataClient dataClient,
            IDataWriter dataWriter,
            IQueueClient queueClient,
            IGroupMemberExtractor groupMemberExtractor,
            IFilterManager filterManager,
            IMetadataStore metadataStore,
            int maxJobCountInRunningPool,
            IMetricsLogger metricsLogger,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirToDataLakeOrchestratorJob> logger)
        {
            _jobInfo = EnsureArg.IsNotNull(jobInfo, nameof(jobInfo));
            _inputData = EnsureArg.IsNotNull(inputData, nameof(inputData));
            _result = EnsureArg.IsNotNull(result, nameof(result));
            _jobSplitter = EnsureArg.IsNotNull(jobSplitter, nameof(jobSplitter));
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            _groupMemberExtractor = EnsureArg.IsNotNull(groupMemberExtractor, nameof(groupMemberExtractor));
            _filterManager = EnsureArg.IsNotNull(filterManager, nameof(filterManager));
            _metadataStore = EnsureArg.IsNotNull(metadataStore, nameof(metadataStore));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
            _maxJobCountInRunningPool = maxJobCountInRunningPool;
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public int InitialOrchestrationIntervalInSeconds { get; set; } = JobConfigurationConstants.DefaultInitialOrchestrationIntervalInSeconds;

        public int IncrementalOrchestrationIntervalInSeconds { get; set; } = JobConfigurationConstants.DefaultIncrementalOrchestrationIntervalInSeconds;

        public int CheckFrequencyInSeconds { get; set; } = JobConfigurationConstants.DefaultCheckFrequencyInSeconds;

        public int NumberOfPatientsPerProcessingJob { get; set; } = JobConfigurationConstants.DefaultNumberOfPatientsPerProcessingJob;

        public async Task<string> ExecuteAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _diagnosticLogger.LogInformation("Start executing FhirToDataLake job.");
            _logger.LogInformation($"Start executing FhirToDataLake orchestrator job {_jobInfo.Id}.");

            try
            {
                if (_inputData.JobVersion >= JobVersion.V4)
                {
                    // Initialize job status from meta table when job version larger than V4.
                    _jobStatus = await InitializeJobStatusFromTableAsync(cancellationToken);
                }
                else
                {
                    _jobStatus = InitializeJobStatusFromResult();
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                FilterScope filterScope = await _filterManager.GetFilterScopeAsync(cancellationToken);
                IAsyncEnumerable<FhirToDataLakeProcessingJobInputData> inputs = filterScope switch
                {
                    // Split job by resource count when job version larger than V4.
                    FilterScope.System => _inputData.JobVersion >= JobVersion.V4 ? GetInputsAsyncForSystem(cancellationToken) : GetInputsAsyncSystemForFixedTimespan(cancellationToken),
                    FilterScope.Group => GetInputsAsyncForGroup(cancellationToken),
                    _ => throw new ConfigurationErrorException(
                        $"The filterScope {filterScope} isn't supported now.")
                };

                await foreach (FhirToDataLakeProcessingJobInputData input in inputs.WithCancellation(cancellationToken))
                {
                    while (GetRunningJobCount() >= _maxJobCountInRunningPool)
                    {
                        await CheckFirstRunningJobComplete(progress, cancellationToken);
                        if (GetRunningJobCount() >= _maxJobCountInRunningPool)
                        {
                            await Task.Delay(TimeSpan.FromSeconds(CheckFrequencyInSeconds), cancellationToken);
                        }
                    }

                    string[] jobDefinitions = { JsonConvert.SerializeObject(input) };

                    _jobStatus.SubmittingProcessingJob = jobDefinitions;
                    await UploadJobStatusAsync(progress, cancellationToken);

                    IEnumerable<JobInfo> jobInfos = await _queueClient.EnqueueAsync(
                        _jobInfo.QueueType,
                        jobDefinitions,
                        _jobInfo.GroupId,
                        false,
                        false,
                        cancellationToken);
                    long newJobId = jobInfos.First().Id;
                    UpdateSubmittingJobStatus(input, newJobId);
                    await UploadJobStatusAsync(progress, cancellationToken);

                    if (GetRunningJobCount() >
                        JobConfigurationConstants.CheckRunningJobCompleteRunningJobCountThreshold)
                    {
                        await CheckFirstRunningJobComplete(progress, cancellationToken);
                    }
                }

                _logger.LogInformation($"Orchestrator job {_jobInfo.Id} finished generating and enqueueing processing jobs.");

                while (GetRunningJobCount() > 0)
                {
                    await CheckFirstRunningJobComplete(progress, cancellationToken);
                    if (GetRunningJobCount() > 0)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(CheckFrequencyInSeconds), cancellationToken);
                    }
                }

                var processEndtime = DateTimeOffset.UtcNow;
                _jobStatus.CompleteTime = processEndtime;

                await UploadJobStatusAsync(progress, cancellationToken);

                var jobLatency = processEndtime - _inputData.DataEndTime;
                _metricsLogger.LogResourceLatencyMetric(jobLatency.TotalSeconds);

                _diagnosticLogger.LogInformation($"Finish FhirToDataLake job. Synced result data to {_inputData.DataEndTime}.");
                _logger.LogInformation($"Finish FhirToDataLake orchestrator job {_jobInfo.Id}. Synced result data to {_inputData.DataEndTime}. " +
                    $"Report a {MetricNames.ResourceLatencyMetric} metric with {jobLatency.TotalSeconds} seconds latency");

                return _inputData.JobVersion >= JobVersion.V4 ? _jobStatusEntity.JobStatus : JsonConvert.SerializeObject(_jobStatus);
            }
            catch (OperationCanceledException operationCanceledEx)
            {
                _diagnosticLogger.LogError("FhirToDataLake job is canceled.");
                _logger.LogInformation(operationCanceledEx, "FhirToDataLake orchestrator job {0} is canceled.", _jobInfo.Id);
                _metricsLogger.LogTotalErrorsMetrics(operationCanceledEx, $"FhirToDataLake orchestrator job is canceled. Reason: {operationCanceledEx.Message}", JobOperations.RunJob);
                throw new RetriableJobException("Job is cancelled.", operationCanceledEx);
            }
            catch (SynapsePipelineExternalException synapsePipelineRetriableEx)
            {
                // Customer exceptions.
                _diagnosticLogger.LogError($"Error in FhirToDataLake job. Reason:{synapsePipelineRetriableEx.Message}");
                _logger.LogInformation(synapsePipelineRetriableEx, "Error in orchestrator job {0}. Reason:{1}", _jobInfo.Id, synapsePipelineRetriableEx);
                _metricsLogger.LogTotalErrorsMetrics(synapsePipelineRetriableEx, $"Error in orchestrator job. Reason: {synapsePipelineRetriableEx.Message}", JobOperations.RunJob);
                throw new RetriableJobException("Error in orchestrator job.", synapsePipelineRetriableEx);
            }
            catch (RetriableJobException retriableJobEx)
            {
                // always throw RetriableJobException
                _diagnosticLogger.LogError($"Error in FhirToDataLake job. Reason:{retriableJobEx.Message}");
                _logger.LogInformation(retriableJobEx, "Error in orchestrator job {0}. Reason:{1}", _jobInfo.Id, retriableJobEx);
                _metricsLogger.LogTotalErrorsMetrics(retriableJobEx, $"Error in orchestrator job. Reason: {retriableJobEx.Message}", JobOperations.RunJob);
                throw;
            }
            catch (SynapsePipelineInternalException synapsePipelineInternalEx)
            {
                _diagnosticLogger.LogError("Internal error occurred in FhirToDataLake job.");
                _logger.LogError(synapsePipelineInternalEx, "Error in orchestrator job {0}. Reason:{1}", _jobInfo.Id, synapsePipelineInternalEx);
                _metricsLogger.LogTotalErrorsMetrics(synapsePipelineInternalEx, $"Error in orchestrator job. Reason: {synapsePipelineInternalEx.Message}", JobOperations.RunJob);
                throw new RetriableJobException("Error in orchestrator job.", synapsePipelineInternalEx);
            }
            catch (Exception unhandledEx)
            {
                // Unhandled exceptions.
                _diagnosticLogger.LogError("Unknown error occurred in FhirToDataLake job.");
                _logger.LogError(unhandledEx, "Unhandled error occurred in orchestrator job {0}. Reason:{1}", _jobInfo.Id, unhandledEx);
                _metricsLogger.LogTotalErrorsMetrics(unhandledEx, $"Unhandled error occurred in orchestrator job. Reason: {unhandledEx.Message}", JobOperations.RunJob);
                throw new RetriableJobException("Unhandled error occurred in orchestrator job.", unhandledEx);
            }
        }

        private void UpdateSubmittingJobStatus(FhirToDataLakeProcessingJobInputData input, long newJobId)
        {
            _jobStatus.CreatedJobCount++;
            if (_inputData.JobVersion == JobVersion.V1 || _inputData.JobVersion == JobVersion.V2)
            {
                _jobStatus.RunningJobIds.Add(newJobId);
            }

            _jobStatus.SequenceIdToJobIdMapForRunningJobs[input.ProcessingJobSequenceId] = newJobId;

            if (input.SplitParameters != null)
            {
                foreach (var param in input.SplitParameters)
                {
                    _jobStatus.SubmittedResourceTimestamps[param.Key] = param.Value.DataEndTime;
                }
            }

            _jobStatus.SubmittingProcessingJob = null;
        }

        private FhirToDataLakeOrchestratorJobStatus InitializeJobStatusFromResult()
        {
            var resultContent = JsonConvert.SerializeObject(_result);
            return JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobStatus>(resultContent);
        }

        private FhirToDataLakeOrchestratorJobResult GenerateJobResultFromStatusAsync()
        {
            var resultContent = JsonConvert.SerializeObject(_jobStatus);
            return JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobResult>(resultContent);
        }

        private async Task<FhirToDataLakeOrchestratorJobStatus> InitializeJobStatusFromTableAsync(CancellationToken cancellationToken)
        {
            _jobStatusEntity = await _metadataStore.GetOrchestratorJobStatusAsync(_jobInfo.QueueType, _jobInfo.GroupId, _jobInfo.Id, cancellationToken);

            if (_jobStatusEntity == null)
            {
                // Orchestrator job status entity is not exist, will create a new one.
                var newJobStatusEntity = new OrchestratorJobStatusEntity()
                {
                    PartitionKey = TableKeyProvider.JobStatusPartitionKey(_jobInfo.QueueType, Convert.ToInt32(JobType.Orchestrator)),
                    RowKey = TableKeyProvider.JobStatusRowKey(_jobInfo.QueueType, Convert.ToInt32(JobType.Orchestrator), _jobInfo.GroupId, _jobInfo.Id),
                    GroupId = _jobInfo.GroupId,
                    JobStatus = JsonConvert.SerializeObject(new FhirToDataLakeOrchestratorJobResult()),
                };

                if (!await _metadataStore.TryAddEntityAsync(newJobStatusEntity, cancellationToken))
                {
                    _logger.LogInformation("Initialize job status failed in orchestrator job {0}.", _jobInfo.Id);
                    throw new SynapsePipelineInternalException($"Initialize job status failed in orchestrator job {_jobInfo.Id}.");
                }

                _jobStatusEntity = await _metadataStore.GetOrchestratorJobStatusAsync(_jobInfo.QueueType, _jobInfo.GroupId, _jobInfo.Id, cancellationToken);
            }

            return JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobStatus>(_jobStatusEntity.JobStatus);
        }

        private async Task UploadJobStatusAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            if (_inputData.JobVersion <= JobVersion.V4)
            {
                progress.Report(JsonConvert.SerializeObject(GenerateJobResultFromStatusAsync()));
            }

            if(_inputData.JobVersion >= JobVersion.V4)
            {
                // Update job status on meta table for job version larger than V4.
                _jobStatusEntity.JobStatus = JsonConvert.SerializeObject(_jobStatus);

                if (!await _metadataStore.TryUpdateEntityAsync(_jobStatusEntity))
                {
                    _logger.LogInformation("Update orchestrator job status failed in job {0}.", _jobInfo.Id);
                    throw new SynapsePipelineInternalException($"Update orchestrator job status failed in job  {_jobInfo.Id}.");
                }

                _jobStatusEntity = await _metadataStore.GetOrchestratorJobStatusAsync(_jobInfo.QueueType, _jobInfo.GroupId, _jobInfo.Id, cancellationToken);
            }
        }

        private async IAsyncEnumerable<FhirToDataLakeProcessingJobInputData> GetInputsAsyncForSystem([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            // Resume the submitting jobs.
            if (_jobStatus.SubmittingProcessingJob != null)
            {
                foreach (var job in _jobStatus.SubmittingProcessingJob)
                {
                    yield return JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobInputData>(job);
                }
            }

            IAsyncEnumerable<FhirToDataLakeSplitProcessingJobInfo> processingJobs = _jobSplitter.SplitJobAsync(_inputData.DataStartTime, _inputData.DataEndTime, _jobStatus.SubmittedResourceTimestamps, cancellationToken);
            await foreach (FhirToDataLakeSplitProcessingJobInfo processingJob in processingJobs.WithCancellation(cancellationToken))
            {
                if (processingJob.ResourceCount == 0)
                {
                    // If resource count is 0, skip the job and update the submitted timestamp.
                    foreach (FhirToDataLakeSplitSubJobInfo subJob in processingJob.SubJobInfos)
                    {
                        _jobStatus.SubmittedResourceTimestamps[subJob.ResourceType] = subJob.TimeRange.DataEndTime;
                    }

                    continue;
                }

                yield return new FhirToDataLakeProcessingJobInputData
                {
                    JobType = JobType.Processing,
                    JobVersion = _inputData.JobVersion,
                    ProcessingJobSequenceId = _jobStatus.CreatedJobCount,
                    TriggerSequenceId = _inputData.TriggerSequenceId,
                    Since = _inputData.Since,
                    SplitParameters = processingJob.SubJobInfos.ToDictionary(x => x.ResourceType, x => x.TimeRange),
                };
            }
        }

        private async IAsyncEnumerable<FhirToDataLakeProcessingJobInputData> GetInputsAsyncSystemForFixedTimespan([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            TimeSpan interval = TimeSpan.FromSeconds(IncrementalOrchestrationIntervalInSeconds);

            if (_inputData.DataStartTime == null || ((DateTimeOffset)_inputData.DataStartTime).AddMinutes(60) < _inputData.DataEndTime)
            {
                interval = TimeSpan.FromSeconds(InitialOrchestrationIntervalInSeconds);
            }

            while (_jobStatus.NextJobTimestamp == null || _jobStatus.NextJobTimestamp < _inputData.DataEndTime)
            {
                DateTimeOffset? lastEndTime = _jobStatus.NextJobTimestamp ?? _inputData.DataStartTime;
                DateTimeOffset? nextResourceTimestamp =
                    await GetNextTimestamp(lastEndTime, _inputData.DataEndTime, cancellationToken);
                if (nextResourceTimestamp == null)
                {
                    _jobStatus.NextJobTimestamp = _inputData.DataEndTime;
                    break;
                }

                DateTimeOffset nextJobEnd = (DateTimeOffset)nextResourceTimestamp + interval;
                if (nextJobEnd > _inputData.DataEndTime)
                {
                    nextJobEnd = _inputData.DataEndTime;
                }

                // For job version compatibility
                // job version v1 use nextResourceTimestamp as the next job start time, job version v2 improve it by using lastEndTime
                DateTimeOffset? nextJobStart = _inputData.JobVersion == JobVersion.V1 ? nextResourceTimestamp : lastEndTime;

                var input = new FhirToDataLakeProcessingJobInputData
                {
                    JobType = JobType.Processing,
                    JobVersion = _inputData.JobVersion,
                    ProcessingJobSequenceId = _jobStatus.CreatedJobCount,
                    TriggerSequenceId = _inputData.TriggerSequenceId,
                    Since = _inputData.Since,
                    DataStartTime = nextJobStart,
                    DataEndTime = nextJobEnd,
                };
                _jobStatus.NextJobTimestamp = nextJobEnd;
                yield return input;
            }
        }

        private async IAsyncEnumerable<FhirToDataLakeProcessingJobInputData> GetInputsAsyncForGroup([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            // for group scope, extract patient list from group at first
            List<PatientWrapper> toBeProcessedPatients = await GetToBeProcessedPatientsAsync(cancellationToken);

            while (_jobStatus.NextPatientIndex < toBeProcessedPatients.Count)
            {
                List<PatientWrapper> selectedPatients = toBeProcessedPatients.Skip(_jobStatus.NextPatientIndex)
                    .Take(NumberOfPatientsPerProcessingJob).ToList();
                var input = new FhirToDataLakeProcessingJobInputData
                {
                    JobType = JobType.Processing,
                    JobVersion = _inputData.JobVersion,
                    ProcessingJobSequenceId = _jobStatus.CreatedJobCount,
                    TriggerSequenceId = _inputData.TriggerSequenceId,
                    Since = _inputData.Since,
                    DataStartTime = _inputData.DataStartTime,
                    DataEndTime = _inputData.DataEndTime,
                    ToBeProcessedPatients = selectedPatients,
                };
                _jobStatus.NextPatientIndex += selectedPatients.Count;
                yield return input;
            }
        }

        private async Task<List<PatientWrapper>> GetToBeProcessedPatientsAsync(CancellationToken cancellationToken)
        {
            string groupID = await _filterManager.GetGroupIdAsync(cancellationToken);

            // extract patient ids from group
            _logger.LogInformation($"Start extracting patients from group '{groupID}'.");

            // For now, the queryParameters is always null.
            // This parameter will be used when we enable filter groups in the future.
            HashSet<string> patientsHash = (await _groupMemberExtractor.GetGroupPatientsAsync(
                groupID,
                null,
                _inputData.DataEndTime,
                cancellationToken)).Select(TableKeyProvider.CompartmentRowKey).ToHashSet();

            Dictionary<string, long> processedPatientVersions;
            try
            {
                processedPatientVersions = await _metadataStore.GetPatientVersionsAsync(_jobInfo.QueueType, patientsHash.ToList(), cancellationToken);
            }
            catch (RequestFailedException ex)
            {
                _logger.LogError(ex, "Failed to get patient versions from metadata table.");
                throw new MetadataStoreException("Failed to get patient versions from metadata table.", ex);
            }

            // set the version id for processed patient
            // the processed patients is empty at the beginning , and will be updated when completing a successful job.
            List<PatientWrapper> toBeProcessedPatients = patientsHash.Select(patientHash =>
                new PatientWrapper(
                    patientHash,
                    processedPatientVersions.ContainsKey(patientHash) ? processedPatientVersions[patientHash] : 0)).ToList();

            _logger.LogInformation(
                "Extract {patientCount} patients from group '{groupId}', including {newPatientCount} new patients.",
                patientsHash.Count,
                groupID,
                toBeProcessedPatients.Where(p => p.VersionId == 0).ToList().Count);

            return toBeProcessedPatients;
        }

        // get the lastUpdated timestamp of next resource for next processing job
        private async Task<DateTimeOffset?> GetNextTimestamp(DateTimeOffset? start, DateTimeOffset end, CancellationToken cancellationToken)
        {
            List<TypeFilter> typeFilters = await _filterManager.GetTypeFiltersAsync(cancellationToken);

            List<KeyValuePair<string, string>> parameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{end.ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Single.ToString("d")),
            };

            if (start != null)
            {
                parameters.Add(new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{((DateTimeOffset)start).ToInstantString()}"));
            }

            var searchOptions = new BaseSearchOptions(null, parameters);

            List<KeyValuePair<string, string>> sharedQueryParameters = new List<KeyValuePair<string, string>>(searchOptions.QueryParameters);

            DateTimeOffset? nexTimeOffset = null;
            foreach (TypeFilter typeFilter in typeFilters)
            {
                searchOptions.ResourceType = typeFilter.ResourceType;
                searchOptions.QueryParameters = new List<KeyValuePair<string, string>>(sharedQueryParameters);
                foreach (KeyValuePair<string, string> parameter in typeFilter.Parameters)
                {
                    searchOptions.QueryParameters.Add(parameter);
                }

                string fhirBundleResult = await _dataClient.SearchAsync(searchOptions, cancellationToken);

                // Parse bundle result.
                JObject fhirBundleObject;
                try
                {
                    fhirBundleObject = JObject.Parse(fhirBundleResult);
                }
                catch (JsonReaderException exception)
                {
                    string reason = string.Format(
                            "Failed to parse fhir search result for '{0}' with search parameters '{1}'.",
                            searchOptions.ResourceType,
                            string.Join(", ", searchOptions.QueryParameters.Select(parameter => $"{parameter.Key}: {parameter.Value}")));

                    _diagnosticLogger.LogError(reason);
                    _logger.LogInformation(exception, reason);
                    throw new FhirDataParseException(reason, exception);
                }

                List<JObject> fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject).ToList();

                if (fhirResources.Any())
                {
                    if (nexTimeOffset == null || fhirResources.First().GetLastUpdated() < nexTimeOffset)
                    {
                        nexTimeOffset = fhirResources.First().GetLastUpdated();
                    }
                }
            }

            return nexTimeOffset;
        }

        private async Task CheckFirstRunningJobComplete(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Orchestrator job {_jobInfo.Id} starts to check the first running job status.");

            long firstRunningJobId = GetFirstRunningJobId();

            JobInfo firstRunningJobInfo = await _queueClient.GetJobByIdAsync(_jobInfo.QueueType, firstRunningJobId, false, cancellationToken);

            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }

            if (firstRunningJobInfo.Status != JobStatus.Created && firstRunningJobInfo.Status != JobStatus.Running)
            {
                if (firstRunningJobInfo.Status == JobStatus.Completed)
                {
                    await CommitJobData(firstRunningJobInfo.Id, cancellationToken);
                    if (firstRunningJobInfo.Result != null)
                    {
                        var processingJobResult =
                            JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobResult>(firstRunningJobInfo.Result);
                        _jobStatus.TotalResourceCounts =
                            _jobStatus.TotalResourceCounts.ConcatDictionaryCount(processingJobResult.SearchCount);
                        _jobStatus.ProcessedResourceCounts =
                            _jobStatus.ProcessedResourceCounts.ConcatDictionaryCount(processingJobResult.ProcessedCount);
                        _jobStatus.SkippedResourceCounts =
                            _jobStatus.SkippedResourceCounts.ConcatDictionaryCount(processingJobResult.SkippedCount);
                        _jobStatus.ProcessedCountInTotal += processingJobResult.ProcessedCountInTotal;
                        _jobStatus.ProcessedDataSizeInTotal += processingJobResult.ProcessedDataSizeInTotal;

                        // log metrics
                        _metricsLogger.LogSuccessfulResourceCountMetric(processingJobResult.ProcessedCountInTotal);
                        _metricsLogger.LogSuccessfulDataSizeMetric(processingJobResult.ProcessedDataSizeInTotal);

                        _logger.LogInformation($"Report a {MetricNames.SuccessfulResourceCountMetric} metric with {processingJobResult.ProcessedCountInTotal} resources.");
                        _logger.LogInformation($"Report a {MetricNames.SuccessfulDataSizeMetric} metric with {processingJobResult.ProcessedDataSizeInTotal} bytes data size.");

                        if (await _filterManager.GetFilterScopeAsync(cancellationToken) == FilterScope.Group)
                        {
                            try
                            {
                                await _metadataStore.UpdatePatientVersionsAsync(
                                _jobInfo.QueueType,
                                processingJobResult.ProcessedPatientVersion,
                                cancellationToken);
                            }
                            catch (RequestFailedException ex)
                            {
                                _logger.LogError(ex, "Failed to update patient versions from metadata table.");
                                throw new MetadataStoreException("Failed to update patient versions from metadata table.", ex);
                            }
                        }
                    }
                }
                else if (firstRunningJobInfo.Status == JobStatus.Failed)
                {
                    _logger.LogInformation("The processing job is failed.");
                    throw new RetriableJobException("The processing job is failed.");
                }
                else if (firstRunningJobInfo.Status == JobStatus.Cancelled)
                {
                    _logger.LogInformation("Operation cancelled by customer.");
                    throw new OperationCanceledException("Operation cancelled by customer.");
                }

                UpdateRunningJobStatus(firstRunningJobId);
                await UploadJobStatusAsync(progress, cancellationToken);
            }

            _logger.LogInformation($"Orchestrator job {_jobInfo.Id} finished checking the first running job {firstRunningJobId} status, the job {firstRunningJobId} is {((JobStatus)firstRunningJobInfo.Status).ToString("D")}.");
        }

        private long GetFirstRunningJobId()
        {
            return _inputData.JobVersion switch
            {
                JobVersion.V1 or JobVersion.V2 => _jobStatus.RunningJobIds.Min(),
                JobVersion.V3 or JobVersion.V4 => _jobStatus.SequenceIdToJobIdMapForRunningJobs[_jobStatus.CompletedJobCount],
                _ => throw new SynapsePipelineInternalException($"The job version {_inputData.JobVersion} is unsupported."),
            };
        }

        private void UpdateRunningJobStatus(long jobId)
        {
            switch (_inputData.JobVersion)
            {
                case JobVersion.V1:
                case JobVersion.V2:
                    _jobStatus.RunningJobIds.Remove(jobId);
                    break;
                case JobVersion.V3:
                case JobVersion.V4:
                    _jobStatus.SequenceIdToJobIdMapForRunningJobs.Remove(_jobStatus.CompletedJobCount);
                    _jobStatus.CompletedJobCount++;
                    break;
                default:
                    throw new SynapsePipelineInternalException($"The job version {_inputData.JobVersion} is unsupported.");
            }
        }

        private int GetRunningJobCount()
        {
            return _inputData.JobVersion switch
            {
                JobVersion.V1 or JobVersion.V2 => _jobStatus.RunningJobIds.Count,
                JobVersion.V3 or JobVersion.V4 => (int)(_jobStatus.CreatedJobCount - _jobStatus.CompletedJobCount),
                _ => throw new SynapsePipelineInternalException($"The job version {_inputData.JobVersion} is unsupported."),
            };
        }

        private async Task CommitJobData(long jobId, CancellationToken cancellationToken)
        {
            // TODO: job Id or job sequence index?
            await _dataWriter.CommitJobDataAsync(jobId, cancellationToken);
        }
    }
}