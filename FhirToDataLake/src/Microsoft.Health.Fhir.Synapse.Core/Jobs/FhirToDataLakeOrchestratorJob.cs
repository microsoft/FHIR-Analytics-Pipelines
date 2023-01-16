﻿// -------------------------------------------------------------------------------------------------
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
using Hl7.Fhir.Utility;
using Hl7.FhirPath.Sprache;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using TypeFilter = Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch.TypeFilter;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class FhirToDataLakeOrchestratorJob : IJob
    {
        private readonly JobInfo _jobInfo;
        private readonly FhirToDataLakeOrchestratorJobInputData _inputData;
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly IQueueClient _queueClient;
        private readonly IGroupMemberExtractor _groupMemberExtractor;
        private readonly IMetadataStore _metadataStore;
        private readonly IFilterManager _filterManager;
        private readonly int _maxJobCountInRunningPool;
        private readonly ILogger<FhirToDataLakeOrchestratorJob> _logger;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly IMetricsLogger _metricsLogger;
        private FhirToDataLakeOrchestratorJobResult _result;
        private OrchestratorJobStatusEntity _jobStatus;
        private List<(string, TimeRange, int)> _mergeList = new List<(string, TimeRange, int)>();
        private FhirToDataLakeProcessingJobSpliter _jobSpliter;

        public FhirToDataLakeOrchestratorJob(
            JobInfo jobInfo,
            FhirToDataLakeOrchestratorJobInputData inputData,
            FhirToDataLakeProcessingJobSpliter jobSpliter,
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
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
            _jobSpliter = EnsureArg.IsNotNull(jobSpliter, nameof(jobSpliter));
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

        public int LowBoundOfProcessingJobResourceCount { get; set; } = JobConfigurationConstants.LowBoundOfProcessingJobResourceCount;

        public int HighBoundOfProcessingJobResourceCount { get; set; } = JobConfigurationConstants.HighBoundOfProcessingJobResourceCount;

        public async Task<string> ExecuteAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _diagnosticLogger.LogInformation("Start executing FhirToDataLake job.");
            _logger.LogInformation($"Start executing FhirToDataLake orchestrator job {_jobInfo.Id}.");

            try
            {
                _jobStatus = await InitializeJobStatusAsync(cancellationToken);
                _result = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobResult>(_jobStatus.StatisticResult);

                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                FilterScope filterScope = await _filterManager.GetFilterScopeAsync(cancellationToken);
                IAsyncEnumerable<FhirToDataLakeProcessingJobInputData> inputs = filterScope switch
                {
                    FilterScope.System => GetInputsAsyncForSystem(cancellationToken),
                    FilterScope.Group => GetInputsAsyncForGroup(cancellationToken),
                    _ => throw new ConfigurationErrorException(
                        $"The filterScope {filterScope} isn't supported now.")
                };

                await foreach (FhirToDataLakeProcessingJobInputData input in inputs.WithCancellation(cancellationToken))
                {
                    while (_result.RunningJobIds.Count >= _maxJobCountInRunningPool)
                    {
                        await CheckRunningJobComplete(progress, cancellationToken);
                        if (_result.RunningJobIds.Count >= _maxJobCountInRunningPool)
                        {
                            await Task.Delay(TimeSpan.FromSeconds(CheckFrequencyInSeconds), cancellationToken);
                        }
                    }

                    string[] jobDefinitions = { JsonConvert.SerializeObject(input) };

                    _result.SubmittingProcessingJob = jobDefinitions;
                    _jobStatus.StatisticResult = JsonConvert.SerializeObject(_result);
                    _jobStatus = await UpdateJobStatusAsync(_jobStatus, cancellationToken);

                    IEnumerable<JobInfo> jobInfos = await _queueClient.EnqueueAsync(
                        _jobInfo.QueueType,
                        jobDefinitions,
                        _jobInfo.GroupId,
                        false,
                        false,
                        cancellationToken);
                    long newJobId = jobInfos.First().Id;
                    _result.CreatedJobCount++;
                    _result.RunningJobIds.Add(newJobId);

                    foreach (var item in input.SplitParameters)
                    {
                        _result.NextJobTimestamp[item.Key] = item.Value.DataEndTime;
                    }

                    _result.SubmittingProcessingJob = null;
                    progress.Report(JsonConvert.SerializeObject(_result));

                    _jobStatus.StatisticResult = JsonConvert.SerializeObject(_result);
                    _jobStatus = await UpdateJobStatusAsync(_jobStatus, cancellationToken);

                    if (_result.RunningJobIds.Count >
                        JobConfigurationConstants.CheckRunningJobCompleteRunningJobCountThreshold)
                    {
                        await CheckRunningJobComplete(progress, cancellationToken);
                    }
                }

                _logger.LogInformation($"Orchestrator job {_jobInfo.Id} finished generating and enqueueing processing jobs.");

                while (_result.RunningJobIds.Count > 0)
                {
                    await CheckRunningJobComplete(progress, cancellationToken);
                    if (_result.RunningJobIds.Count > 0)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(CheckFrequencyInSeconds), cancellationToken);
                    }
                }

                _result.CompleteTime = DateTimeOffset.UtcNow;

                progress.Report(JsonConvert.SerializeObject(_result));
                _jobStatus.StatisticResult = JsonConvert.SerializeObject(_result);
                _jobStatus = await UpdateJobStatusAsync(_jobStatus, cancellationToken);

                _diagnosticLogger.LogInformation("Finish FhirToDataLake job.");
                _logger.LogInformation($"Finish FhirToDataLake orchestrator job {_jobInfo.Id}");

                return JsonConvert.SerializeObject(_result);
            }
            catch (OperationCanceledException operationCanceledEx)
            {
                _diagnosticLogger.LogError("FhirToDataLake job is canceled.");
                _logger.LogInformation(operationCanceledEx, "FhirToDataLake orchestrator job {0} is canceled.", _jobInfo.Id);
                _metricsLogger.LogTotalErrorsMetrics(operationCanceledEx, $"FhirToDataLake orchestrator job is canceled. Reason: {operationCanceledEx.Message}", Operations.RunJob);
                throw new RetriableJobException("Job is cancelled.", operationCanceledEx);
            }
            catch (SynapsePipelineExternalException synapsePipelineRetriableEx)
            {
                // Customer exceptions.
                _diagnosticLogger.LogError($"Error in FhirToDataLake job. Reason:{synapsePipelineRetriableEx.Message}");
                _logger.LogInformation(synapsePipelineRetriableEx, "Error in orchestrator job {0}. Reason:{1}", _jobInfo.Id, synapsePipelineRetriableEx);
                _metricsLogger.LogTotalErrorsMetrics(synapsePipelineRetriableEx, $"Error in orchestrator job. Reason: {synapsePipelineRetriableEx.Message}", Operations.RunJob);
                throw new RetriableJobException("Error in orchestrator job.", synapsePipelineRetriableEx);
            }
            catch (RetriableJobException retriableJobEx)
            {
                // always throw RetriableJobException
                _diagnosticLogger.LogError($"Error in FhirToDataLake job. Reason:{retriableJobEx.Message}");
                _logger.LogInformation(retriableJobEx, "Error in orchestrator job {0}. Reason:{1}", _jobInfo.Id, retriableJobEx);
                _metricsLogger.LogTotalErrorsMetrics(retriableJobEx, $"Error in orchestrator job. Reason: {retriableJobEx.Message}", Operations.RunJob);
                throw;
            }
            catch (SynapsePipelineInternalException synapsePipelineInternalEx)
            {
                _diagnosticLogger.LogError("Internal error occurred in FhirToDataLake job.");
                _logger.LogError(synapsePipelineInternalEx, "Error in orchestrator job {0}. Reason:{1}", _jobInfo.Id, synapsePipelineInternalEx);
                _metricsLogger.LogTotalErrorsMetrics(synapsePipelineInternalEx, $"Error in orchestrator job. Reason: {synapsePipelineInternalEx.Message}", Operations.RunJob);
                throw new RetriableJobException("Error in orchestrator job.", synapsePipelineInternalEx);
            }
            catch (Exception unhandledEx)
            {
                // Unhandled exceptions.
                _diagnosticLogger.LogError("Unknown error occurred in FhirToDataLake job.");
                _logger.LogError(unhandledEx, "Unhandled error occurred in orchestrator job {0}. Reason:{1}", _jobInfo.Id, unhandledEx);
                _metricsLogger.LogTotalErrorsMetrics(unhandledEx, $"Unhandled error occurred in orchestrator job. Reason: {unhandledEx.Message}", Operations.RunJob);
                throw new RetriableJobException("Unhandled error occurred in orchestrator job.", unhandledEx);
            }
        }

        private async Task<OrchestratorJobStatusEntity> InitializeJobStatusAsync(CancellationToken cancellationToken)
        {
            OrchestratorJobStatusEntity jobStatus = await _metadataStore.GetOrchestratorJobStatusAsync(_jobInfo.QueueType, _jobInfo.GroupId, cancellationToken);

            if (jobStatus == null)
            {
                var newJobStatus = new OrchestratorJobStatusEntity()
                {
                    PartitionKey = TableKeyProvider.JobStatusPartitionKey(_jobInfo.QueueType, JobType.Orchestrator.ToString()),
                    RowKey = TableKeyProvider.JobStatusRowKey(_jobInfo.QueueType, JobType.Orchestrator.ToString(), _jobInfo.GroupId),
                    GroupId = _jobInfo.GroupId,
                    StatisticResult = JsonConvert.SerializeObject(new FhirToDataLakeOrchestratorJobResult()),
                };

                if (!await _metadataStore.TryAddEntityAsync(newJobStatus, cancellationToken))
                {
                    throw new SynapsePipelineInternalException("Initialize job status failed");
                }

                jobStatus = await _metadataStore.GetOrchestratorJobStatusAsync(_jobInfo.QueueType, _jobInfo.GroupId, cancellationToken);
            }

            return jobStatus;
        }

        private async Task<OrchestratorJobStatusEntity> UpdateJobStatusAsync(OrchestratorJobStatusEntity jobStatus, CancellationToken cancellationToken)
        {
            if (!await _metadataStore.TryUpdateEntityAsync(_jobStatus))
            {
                throw new SynapsePipelineInternalException("Update orchestrator job status failed.");
            }

            return await _metadataStore.GetOrchestratorJobStatusAsync(_jobInfo.QueueType, _jobInfo.GroupId, cancellationToken);
        }

        private async IAsyncEnumerable<FhirToDataLakeProcessingJobInputData> GetInputsAsyncForSystem([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            // Resume the submitting job.
            if (_result.SubmittingProcessingJob != null)
            {
                yield return JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobInputData>(_result.SubmittingProcessingJob.First());
            }

            List<TypeFilter> typeFilters = await _filterManager.GetTypeFiltersAsync(cancellationToken);
            var resourceTypes = typeFilters.Select(filter => filter.ResourceType).ToList();

            // Split jobs by resource types.
            foreach (var resourceType in resourceTypes)
            {
                var startTime = _result.NextJobTimestamp.ContainsKey(resourceType) ? _result.NextJobTimestamp[resourceType] : _inputData.DataStartTime;

                // The resource type has already been processed.
                if (startTime >= _inputData.DataEndTime)
                {
                    continue;
                }

                IAsyncEnumerable<FhirToDataLakeProcessingJobParameters> jobs = _jobSpliter.SplitJobAsync(resourceType, startTime, _inputData.DataEndTime, cancellationToken);

                await foreach (FhirToDataLakeProcessingJobParameters input in jobs.WithCancellation(cancellationToken))
                {
                    if (input.JobSize == 0)
                    {
                        _result.NextJobTimestamp[resourceType] = _inputData.DataEndTime;
                        continue;
                    }
                    else if (input.JobSize < LowBoundOfProcessingJobResourceCount)
                    {
                        // Small size job, put it into pool and wait for merge.
                        _logger.LogInformation($"Splitting jobs. Generated one small job for {resourceType} with {input.JobSize} count.");

                        var jobParameters = PushToJobPool(resourceType, input.TimeRange, input.JobSize);
                        if (jobParameters != null)
                        {
                            yield return new FhirToDataLakeProcessingJobInputData
                            {
                                JobType = JobType.Processing,
                                ProcessingJobSequenceId = _result.CreatedJobCount,
                                TriggerSequenceId = _inputData.TriggerSequenceId,
                                Since = _inputData.Since,
                                SplitParameters = jobParameters,
                            };
                        }
                    }
                    else
                    {
                        yield return new FhirToDataLakeProcessingJobInputData
                        {
                            JobType = JobType.Processing,
                            ProcessingJobSequenceId = _result.CreatedJobCount,
                            TriggerSequenceId = _inputData.TriggerSequenceId,
                            Since = _inputData.Since,
                            SplitParameters = new Dictionary<string, TimeRange>() { { input.ResourceType, input.TimeRange } },
                        };
                    }
                }
            }

            if (_mergeList.Count != 0)
            {
                var jobParameters = _mergeList.ToDictionary(x => x.Item1, x => x.Item2);

                yield return new FhirToDataLakeProcessingJobInputData
                {
                    JobType = JobType.Processing,
                    ProcessingJobSequenceId = _result.CreatedJobCount,
                    TriggerSequenceId = _inputData.TriggerSequenceId,
                    Since = _inputData.Since,
                    SplitParameters = jobParameters,
                };
            }
        }

        private Dictionary<string, TimeRange> PushToJobPool(string resourceType, TimeRange range, int count)
        {
            // Push small job into pool list.
            var current = _mergeList.Sum(x => x.Item3);
            if (current + count > LowBoundOfProcessingJobResourceCount)
            {
                // Pop all jobs if total resource count larger than low bound.
                var result = _mergeList.ToDictionary(x => x.Item1, x => x.Item2);
                result.Add(resourceType, range);
                _mergeList.Clear();
                return result;
            }

            _mergeList.Add((resourceType, range, count));
            return null;
        }

        private async IAsyncEnumerable<FhirToDataLakeProcessingJobInputData> GetInputsAsyncForGroup([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            // for group scope, extract patient list from group at first
            List<PatientWrapper> toBeProcessedPatients = await GetToBeProcessedPatientsAsync(cancellationToken);

            while (_result.NextPatientIndex < toBeProcessedPatients.Count)
            {
                List<PatientWrapper> selectedPatients = toBeProcessedPatients.Skip(_result.NextPatientIndex)
                    .Take(NumberOfPatientsPerProcessingJob).ToList();
                var input = new FhirToDataLakeProcessingJobInputData
                {
                    JobType = JobType.Processing,
                    ProcessingJobSequenceId = _result.CreatedJobCount,
                    TriggerSequenceId = _inputData.TriggerSequenceId,
                    Since = _inputData.Since,
                    DataStartTime = _inputData.DataStartTime,
                    DataEndTime = _inputData.DataEndTime,
                    ToBeProcessedPatients = selectedPatients,
                };
                _result.NextPatientIndex += selectedPatients.Count;
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

        private async Task CheckRunningJobComplete(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Orchestrator job {_jobInfo.Id} starts to check running job status.");

            HashSet<long> completedJobIds = new HashSet<long>();
            List<JobInfo> runningJobs = new List<JobInfo>();

            runningJobs.AddRange(await _queueClient.GetJobsByIdsAsync(_jobInfo.QueueType, _result.RunningJobIds.ToArray(), false, cancellationToken));

            foreach (JobInfo latestJobInfo in runningJobs)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                if (latestJobInfo.Status != JobStatus.Created && latestJobInfo.Status != JobStatus.Running)
                {
                    if (latestJobInfo.Status == JobStatus.Completed)
                    {
                        await CommitJobData(latestJobInfo.Id, cancellationToken);
                        if (latestJobInfo.Result != null)
                        {
                            var processingJobResult =
                                JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobResult>(latestJobInfo.Result);
                            _result.TotalResourceCounts =
                                _result.TotalResourceCounts.ConcatDictionaryCount(processingJobResult.SearchCount);
                            _result.ProcessedResourceCounts =
                                _result.ProcessedResourceCounts.ConcatDictionaryCount(processingJobResult.ProcessedCount);
                            _result.SkippedResourceCounts =
                                _result.SkippedResourceCounts.ConcatDictionaryCount(processingJobResult.SkippedCount);
                            _result.ProcessedCountInTotal += processingJobResult.ProcessedCountInTotal;
                            _result.ProcessedDataSizeInTotal += processingJobResult.ProcessedDataSizeInTotal;

                            // log metrics
                            _metricsLogger.LogSuccessfulResourceCountMetric(processingJobResult.ProcessedCountInTotal);
                            _metricsLogger.LogSuccessfulDataSizeMetric(processingJobResult.ProcessedDataSizeInTotal);

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
                    else if (latestJobInfo.Status == JobStatus.Failed)
                    {
                        _logger.LogInformation("The processing job is failed.");
                        throw new RetriableJobException("The processing job is failed.");
                    }
                    else if (latestJobInfo.Status == JobStatus.Cancelled)
                    {
                        _logger.LogInformation("Operation cancelled by customer.");
                        throw new OperationCanceledException("Operation cancelled by customer.");
                    }

                    completedJobIds.Add(latestJobInfo.Id);
                }
            }

            if (completedJobIds.Count > 0)
            {
                _result.RunningJobIds.ExceptWith(completedJobIds);
                progress.Report(JsonConvert.SerializeObject(_result));

                _jobStatus.StatisticResult = JsonConvert.SerializeObject(_result);
                _jobStatus = await UpdateJobStatusAsync(_jobStatus, cancellationToken);
            }

            _logger.LogInformation($"Orchestrator job {_jobInfo.Id} finished checking running job status, there are {completedJobIds.Count} jobs completed.");
        }

        private async Task CommitJobData(long jobId, CancellationToken cancellationToken)
        {
            // TODO: job Id or job sequence index?
            await _dataWriter.CommitJobDataAsync(jobId, cancellationToken);
        }
    }
}