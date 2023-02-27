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
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;
using Microsoft.Health.AnalyticsConnector.Core.Extensions;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataClient.Models;
using Microsoft.Health.AnalyticsConnector.DataWriter;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public class FhirDataLakeToDataLakeOrchestratorJob : IJob
    {
        private readonly JobInfo _jobInfo;
        private readonly FhirDataLakeToDataLakeOrchestratorJobInputData _inputData;
        private readonly IDataLakeClient _dataLakeClient;
        private readonly IDataWriter _dataWriter;
        private readonly IQueueClient _queueClient;
        private readonly ICompletedBlobStore _completedBlobStore;
        private readonly int _maxJobCountInRunningPool;
        private readonly ILogger<FhirDataLakeToDataLakeOrchestratorJob> _logger;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly IMetricsLogger _metricsLogger;

        private FhirDataLakeToDataLakeOrchestratorJobResult _result;

        public FhirDataLakeToDataLakeOrchestratorJob(
            JobInfo jobInfo,
            FhirDataLakeToDataLakeOrchestratorJobInputData inputData,
            FhirDataLakeToDataLakeOrchestratorJobResult result,
            IDataLakeClient dataLakeClient,
            IDataWriter dataWriter,
            IQueueClient queueClient,
            ICompletedBlobStore completedBlobStore,
            int maxJobCountInRunningPool,
            IMetricsLogger metricsLogger,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirDataLakeToDataLakeOrchestratorJob> logger)
        {
            _jobInfo = EnsureArg.IsNotNull(jobInfo, nameof(jobInfo));
            _inputData = EnsureArg.IsNotNull(inputData, nameof(inputData));
            _result = EnsureArg.IsNotNull(result, nameof(result));
            _dataLakeClient = EnsureArg.IsNotNull(dataLakeClient, nameof(dataLakeClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _queueClient = EnsureArg.IsNotNull(queueClient, nameof(queueClient));
            _completedBlobStore = EnsureArg.IsNotNull(completedBlobStore, nameof(completedBlobStore));
            _maxJobCountInRunningPool = maxJobCountInRunningPool;
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public int CheckFrequencyInSeconds { get; set; } = JobConfigurationConstants.DefaultCheckFrequencyInSeconds;

        public async Task<string> ExecuteAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _diagnosticLogger.LogInformation("Start executing FhirDataLakeToDataLake job.");
            _logger.LogInformation($"Start executing FhirDataLakeToDataLake orchestrator job {_jobInfo.Id}.");

            try
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                IAsyncEnumerable<FhirDataLakeToDataLakeProcessingJobInputData> inputs = GetInputsAsync(cancellationToken);

                await foreach (FhirDataLakeToDataLakeProcessingJobInputData input in inputs.WithCancellation(cancellationToken))
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

                    // if enqueue successfully while fails to report result, will re-enqueue and return the existing jobInfo
                    progress.Report(JsonConvert.SerializeObject(_result));

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

                var processEndtime = DateTimeOffset.UtcNow;
                _result.CompleteTime = processEndtime;

                progress.Report(JsonConvert.SerializeObject(_result));

                var jobLatency = processEndtime - _inputData.DataEndTime;
                _metricsLogger.LogResourceLatencyMetric(jobLatency.TotalSeconds);

                _diagnosticLogger.LogInformation($"Finish FhirDataLakeToDataLake job.");
                _logger.LogInformation($"Finish FhirDataLakeToDataLake orchestrator job {_jobInfo.Id}. " +
                    $"Report a {MetricNames.ResourceLatencyMetric} metric with {jobLatency.TotalSeconds} seconds latency");

                return JsonConvert.SerializeObject(_result);
            }
            catch (OperationCanceledException operationCanceledEx)
            {
                _diagnosticLogger.LogError("FhirDataLakeToDataLake job is canceled.");
                _logger.LogInformation(operationCanceledEx, "FhirDataLakeToDataLake orchestrator job {0} is canceled.", _jobInfo.Id);
                _metricsLogger.LogTotalErrorsMetrics(operationCanceledEx, $"FhirDataLakeToDataLake orchestrator job is canceled. Reason: {operationCanceledEx.Message}", JobOperations.RunJob);
                throw new RetriableJobException("Job is cancelled.", operationCanceledEx);
            }
            catch (SynapsePipelineExternalException synapsePipelineRetriableEx)
            {
                // Customer exceptions.
                _diagnosticLogger.LogError($"Error in FhirDataLakeToDataLake job. Reason:{synapsePipelineRetriableEx.Message}");
                _logger.LogInformation(synapsePipelineRetriableEx, "Error in orchestrator job {0}. Reason:{1}", _jobInfo.Id, synapsePipelineRetriableEx);
                _metricsLogger.LogTotalErrorsMetrics(synapsePipelineRetriableEx, $"Error in orchestrator job. Reason: {synapsePipelineRetriableEx.Message}", JobOperations.RunJob);
                throw new RetriableJobException("Error in orchestrator job.", synapsePipelineRetriableEx);
            }
            catch (RetriableJobException retriableJobEx)
            {
                // always throw RetriableJobException
                _diagnosticLogger.LogError($"Error in FhirDataLakeToDataLake job. Reason:{retriableJobEx.Message}");
                _logger.LogInformation(retriableJobEx, "Error in orchestrator job {0}. Reason:{1}", _jobInfo.Id, retriableJobEx);
                _metricsLogger.LogTotalErrorsMetrics(retriableJobEx, $"Error in orchestrator job. Reason: {retriableJobEx.Message}", JobOperations.RunJob);
                throw;
            }
            catch (SynapsePipelineInternalException synapsePipelineInternalEx)
            {
                _diagnosticLogger.LogError("Internal error occurred in FhirDataLakeToDataLake job.");
                _logger.LogError(synapsePipelineInternalEx, "Error in orchestrator job {0}. Reason:{1}", _jobInfo.Id, synapsePipelineInternalEx);
                _metricsLogger.LogTotalErrorsMetrics(synapsePipelineInternalEx, $"Error in orchestrator job. Reason: {synapsePipelineInternalEx.Message}", JobOperations.RunJob);
                throw new RetriableJobException("Error in orchestrator job.", synapsePipelineInternalEx);
            }
            catch (Exception unhandledEx)
            {
                // Unhandled exceptions.
                _diagnosticLogger.LogError("Unknown error occurred in FhirDataLakeToDataLake job.");
                _logger.LogError(unhandledEx, "Unhandled error occurred in orchestrator job {0}. Reason:{1}", _jobInfo.Id, unhandledEx);
                _metricsLogger.LogTotalErrorsMetrics(unhandledEx, $"Unhandled error occurred in orchestrator job. Reason: {unhandledEx.Message}", JobOperations.RunJob);
                throw new RetriableJobException("Unhandled error occurred in orchestrator job.", unhandledEx);
            }
        }

        private async IAsyncEnumerable<FhirDataLakeToDataLakeProcessingJobInputData> GetInputsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var baseDataLakeOptions = new BaseDataLakeOptions
            {
                EndTime = _inputData.DataEndTime,
            };

            if (_inputData.DataStartTime != null)
            {
                baseDataLakeOptions.StartTime = _inputData.DataStartTime;
            }

            var blobs = await _dataLakeClient.ListBlobsAsync(baseDataLakeOptions, cancellationToken);

            if (blobs.Any())
            {
                var completedBlobs = await _completedBlobStore.GetCompletedBlobsAsync(cancellationToken);
                blobs = blobs.Except(completedBlobs).ToList();

                foreach (var blob in blobs)
                {
                    var input = new FhirDataLakeToDataLakeProcessingJobInputData
                    {
                        JobType = JobType.Processing,
                        JobVersion = _inputData.JobVersion,
                        TriggerSequenceId = _inputData.TriggerSequenceId,
                        BlobName = blob.Name,
                        ETag = blob.ETag,
                    };

                    yield return input;
                }
            }
        }

        private async Task CheckRunningJobComplete(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Orchestrator job {_jobInfo.Id} starts to check running job status.");

            HashSet<long> completedJobIds = new HashSet<long>();
            List<JobInfo> runningJobs = new List<JobInfo>();

            runningJobs.AddRange(await _queueClient.GetJobsByIdsAsync(_jobInfo.QueueType, _result.RunningJobIds.ToArray(), true, cancellationToken));

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
                        var latestJobInputData = JsonConvert.DeserializeObject<FhirDataLakeToDataLakeProcessingJobInputData>(latestJobInfo.Definition);
                        await CreateCompletedBlobEntityAsync(latestJobInputData.BlobName, latestJobInputData.ETag, cancellationToken);

                        if (latestJobInfo.Result != null)
                        {
                            var processingJobResult =
                                JsonConvert.DeserializeObject<FhirDataLakeToDataLakeProcessingJobResult>(latestJobInfo.Result);
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
                        }
                    }
                    else if (latestJobInfo.Status == JobStatus.Failed)
                    {
                        _logger.LogInformation("The processing job is failed.");
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
            }

            _logger.LogInformation($"Orchestrator job {_jobInfo.Id} finished checking running job status, there are {completedJobIds.Count} jobs completed.");
        }

        private async Task CommitJobData(long jobId, CancellationToken cancellationToken)
        {
            // TODO: job Id or job sequence index?
            await _dataWriter.CommitJobDataAsync(jobId, cancellationToken);
        }

        private async Task<bool> CreateCompletedBlobEntityAsync(string blobName, string eTag, CancellationToken cancellationToken)
        {
            var completedBlobEntity = new CompletedBlobEntity
            {
                PartitionKey = blobName,
                RowKey = eTag,
            };

            bool isSucceeded = await _completedBlobStore.TryAddEntityAsync(completedBlobEntity, cancellationToken);

            _logger.LogInformation(isSucceeded
                ? "Create completed blob entity successfully."
                : "Failed to create completed blob entity.");

            return isSucceeded;
        }
    }
}