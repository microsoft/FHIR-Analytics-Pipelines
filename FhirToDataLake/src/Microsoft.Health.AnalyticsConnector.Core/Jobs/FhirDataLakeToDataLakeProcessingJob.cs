// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;
using Microsoft.Health.AnalyticsConnector.Common.Models.Data;
using Microsoft.Health.AnalyticsConnector.Core.DataProcessor;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.Core.Extensions;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataWriter;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public class FhirDataLakeToDataLakeProcessingJob : IJob
    {
        private readonly FhirDataLakeToDataLakeProcessingJobInputData _inputData;
        private readonly IDataLakeClient _dataLakeClient;
        private readonly IDataWriter _dataWriter;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly ISchemaManager<ParquetSchemaNode> _schemaManager;
        private readonly IMetricsLogger _metricsLogger;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<FhirDataLakeToDataLakeProcessingJob> _logger;

        private readonly long _jobId;
        private FhirDataLakeToDataLakeProcessingJobResult _result;
        private CacheResult _cacheResult;

        /// <summary>
        /// Output file index map for all resources/schemas.
        /// The value of each schemaType will be appended to output files.
        /// The format is '{SchemaType}_{index:d10}.parquet', e.g. Patient_0000000001.parquet.
        /// </summary>
        private Dictionary<string, int> _outputFileIndexMap;

        private const int MaxBatchSize = JobConfigurationConstants.NumberOfResourcesPerCommit;
        private static readonly int MaxConcurrentCount = Environment.ProcessorCount * 2;

        public FhirDataLakeToDataLakeProcessingJob(
            long jobId,
            FhirDataLakeToDataLakeProcessingJobInputData inputData,
            FhirDataLakeToDataLakeProcessingJobResult result,
            IDataLakeClient dataLakeClient,
            IDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            ISchemaManager<ParquetSchemaNode> schemaManager,
            IMetricsLogger metricsLogger,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirDataLakeToDataLakeProcessingJob> logger)
        {
            _jobId = jobId;
            _inputData = EnsureArg.IsNotNull(inputData, nameof(inputData));
            _result = EnsureArg.IsNotNull(result, nameof(result));
            _dataLakeClient = EnsureArg.IsNotNull(dataLakeClient, nameof(dataLakeClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _schemaManager = EnsureArg.IsNotNull(schemaManager, nameof(schemaManager));
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public async Task<string> ExecuteAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start executing processing job {_jobId}.");
            string lease = string.Empty;

            try
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Job is cancelled.");
                    throw new OperationCanceledException();
                }

                // clean resources before process start
                await CleanResourceAsync(cancellationToken);

                _cacheResult = new CacheResult();
                _outputFileIndexMap = new Dictionary<string, int>();

                // Acquire lease on blob, lease will be null if blob does not exist
                // TODO [boywu]: check lease timespan
                lease = await _dataLakeClient.AcquireLeaseAsync(_inputData.BlobName, null, TimeSpan.FromSeconds(-1), cancellationToken);

                await ExecuteAsyncInternal(progress, cancellationToken);

                // force to commit result when all the resources of this job are processed.
                await TryCommitResultAsync(progress, true, cancellationToken);

                _result.ProcessingCompleteTime = DateTime.UtcNow;

                progress.Report(JsonConvert.SerializeObject(_result));

                _logger.LogInformation($"Finished processing job {_jobId}.");

                return JsonConvert.SerializeObject(_result);
            }
            catch (OperationCanceledException operationCanceledEx)
            {
                _logger.LogInformation(operationCanceledEx, "Processing job {0} is canceled.", _jobId);
                _metricsLogger.LogTotalErrorsMetrics(operationCanceledEx, $"Processing job is canceled. Reason: {operationCanceledEx.Message}", JobOperations.RunJob);
                await CleanResourceAsync(CancellationToken.None);

                throw new RetriableJobException("Processing job is canceled.", operationCanceledEx);
            }
            catch (RetriableJobException retriableJobEx)
            {
                _logger.LogInformation(retriableJobEx, "Error in processing job {0}. Reason : {1}", _jobId, retriableJobEx.Message);
                _metricsLogger.LogTotalErrorsMetrics(retriableJobEx, $"Error in processing job. Reason: {retriableJobEx.Message}", JobOperations.RunJob);
                await CleanResourceAsync(CancellationToken.None);

                throw;
            }
            catch (SynapsePipelineExternalException synapsePipelineEx)
            {
                // Customer exceptions.
                _logger.LogInformation(synapsePipelineEx, "Error in data processing job {0}. Reason:{1}", _jobId, synapsePipelineEx.Message);
                _metricsLogger.LogTotalErrorsMetrics(synapsePipelineEx, $"Error in processing job. Reason: {synapsePipelineEx.Message}", JobOperations.RunJob);
                await CleanResourceAsync(CancellationToken.None);

                throw new RetriableJobException("Error in data processing job.", synapsePipelineEx);
            }
            catch (JobExecutionException jobExecutionEx)
            {
                // Job execution exceptions. This exception will make the job fail.
                _logger.LogInformation(jobExecutionEx, "Error in processing job {0}. Reason : {1}", _jobId, jobExecutionEx.Message);
                _metricsLogger.LogTotalErrorsMetrics(jobExecutionEx, $"Error in processing job. Reason: {jobExecutionEx.Message}", JobOperations.RunJob);
                await CleanResourceAsync(CancellationToken.None);

                throw;
            }
            catch (Exception ex)
            {
                // Unhandled exceptions.
                _logger.LogError(ex, "Unhandled error occurred in data processing job {0}. Reason : {1}", _jobId, ex.Message);
                _metricsLogger.LogTotalErrorsMetrics(ex, $"Unhandled error occurred in data processing job. Reason: {ex.Message}", JobOperations.RunJob);
                await CleanResourceAsync(CancellationToken.None);

                throw new RetriableJobException("Unhandled error occurred in data processing job.", ex);
            }
            finally
            {
                await _dataLakeClient.ReleaseLeaseAsync(_inputData.BlobName, lease, cancellationToken);

                // Compact large objects allocated in processing.
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect();
            }
        }

        private async Task ExecuteAsyncInternal(IProgress<string> progress, CancellationToken cancellationToken)
        {
            // Job fail if blob is updated
            var currentETag = _dataLakeClient.GetBlobETag(_inputData.BlobName);
            if (currentETag != _inputData.ETag)
            {
                throw new JobExecutionException("The blob is updated when processing.", new Exception());
            }

            using Stream inputDataStream = await _dataLakeClient.LoadBlobAsync(_inputData.BlobName, cancellationToken);
            using StreamReader inputDataReader = new StreamReader(inputDataStream);

            string content = null;
            long currentIndex = 0;
            List<JObject> buffer = new List<JObject>();
            Queue<Task> processingTasks = new Queue<Task>();

            // StreamReader exceptions are retriable
            while (!string.IsNullOrEmpty(content = await inputDataReader.ReadLineAsync()))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                buffer.Add(ParseResource(content, _inputData.BlobName, _inputData.ETag, currentIndex));
                currentIndex++;
                _cacheResult.CacheSize += content.Length * sizeof(char);

                if (buffer.Count < MaxBatchSize)
                {
                    continue;
                }

                while (processingTasks.Count >= MaxConcurrentCount)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new OperationCanceledException();
                    }

                    await processingTasks.Dequeue();
                }

                AddFhirResourcesToCache(buffer);
                processingTasks.Enqueue(TryCommitResultAsync(progress, false, cancellationToken));
                buffer.Clear();
            }

            AddFhirResourcesToCache(buffer);
            processingTasks.Enqueue(TryCommitResultAsync(progress, false, cancellationToken));

            while (processingTasks.Count > 0)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                await processingTasks.Dequeue();
            }
        }

        /// <summary>
        /// Add FHIR resources to memory cache.
        /// </summary>
        private void AddFhirResourcesToCache(List<JObject> fhirResources)
        {
            foreach (JObject resource in fhirResources)
            {
                string resourceType = resource["resourceType"]?.ToString();
                if (string.IsNullOrWhiteSpace(resourceType))
                {
                    _diagnosticLogger.LogError($"Failed to parse fhir search resource {resource}");
                    _logger.LogInformation($"Failed to parse fhir search resource {resource}");
                    throw new FhirDataParseException($"Failed to parse fhir search resource {resource}");
                }

                if (!_cacheResult.Resources.ContainsKey(resourceType))
                {
                    _cacheResult.Resources[resourceType] = new List<JObject>();
                }

                _cacheResult.Resources[resourceType].Add(resource);
            }
        }

        /// <summary>
        /// Try to commit the resources in memory cache to storage,
        /// if resources are committed,
        /// 1. clear the cache resources
        /// 2. update and report _result
        /// </summary>
        private async Task TryCommitResultAsync(
            IProgress<string> progress,
            bool forceCommit,
            CancellationToken cancellationToken)
        {
            int cacheResourceCount = _cacheResult.GetResourceCount();
            if (cacheResourceCount >= JobConfigurationConstants.NumberOfResourcesPerCommit ||
                _cacheResult.CacheSize > JobConfigurationConstants.DataSizeInBytesPerCommit ||
                forceCommit)
            {
                _logger.LogInformation($"commit data: {cacheResourceCount} resources, {_cacheResult.CacheSize} data size.");

                foreach ((string resourceType, List<JObject> resources) in _cacheResult.Resources)
                {
                    var batchData = new JsonBatchData(resources);

                    List<string> schemaTypes = _schemaManager.GetSchemaTypes(resourceType);
                    foreach (string schemaType in schemaTypes)
                    {
                        // Convert grouped data to parquet stream
                        var processParameters = new ProcessParameters(schemaType, resourceType);
                        using StreamBatchData parquetStream = await _parquetDataProcessor.ProcessAsync(batchData, processParameters, cancellationToken);
                        int skippedCount = batchData.Values.Count() - parquetStream.BatchSize;

                        if (parquetStream.Value?.Length > 0)
                        {
                            if (!_outputFileIndexMap.ContainsKey(schemaType))
                            {
                                _outputFileIndexMap[schemaType] = 0;
                            }

                            // Upload to blob and log result
                            var fileName = GetDataFileName(_jobId, schemaType, _inputData.BlobName, _inputData.ETag, _outputFileIndexMap[schemaType]);
                            string blobUrl = await _dataWriter.WriteAsync(parquetStream, fileName, cancellationToken);
                            _outputFileIndexMap[schemaType] += 1;

                            _result.ProcessedDataSizeInTotal += parquetStream.Value.Length;

                            _logger.LogInformation(
                                "Commit Batch Result: resource type {resourceType}, schemaType {schemaType}, {resourceCount} resources are searched in total. {processedCount} resources are processed. The blob URL is {blobURL}",
                                resourceType,
                                schemaType,
                                batchData.Values.Count(),
                                parquetStream.BatchSize,
                                blobUrl);
                        }
                        else
                        {
                            _logger.LogInformation(
                                "No resource of schema type {schemaType} from {resourceType} is processed. {skippedCount} resources are skipped.",
                                schemaType,
                                resourceType,
                                skippedCount);
                        }

                        _result.SkippedCount =
                            _result.SkippedCount.AddToDictionary(schemaType, skippedCount);
                        _result.ProcessedCount =
                            _result.ProcessedCount.AddToDictionary(schemaType, parquetStream.BatchSize);
                    }

                    _result.SearchCount =
                        _result.SearchCount.AddToDictionary(resourceType, resources.Count);
                }

                _result.ProcessedCountInTotal = _result.ProcessedCount.Sum(x => x.Value);

                progress.Report(JsonConvert.SerializeObject(_result));

                // clear cache
                _cacheResult.ClearCache();
                _logger.LogInformation($"Commit cache resources successfully for processing job {_jobId}.");
            }
        }

        /// <summary>
        /// Try best to clean failure data.
        /// </summary>
        private async Task CleanResourceAsync(CancellationToken cancellationToken)
        {
            try
            {
                await _dataWriter.TryCleanJobDataAsync(_jobId, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Failed to clean resource.");
            }
        }

        private static string GetDataFileName(
            long jobId,
            string schemaType,
            string blobName,
            string eTag,
            int partId)
        {
            return $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}/{blobName}/{eTag}/{schemaType}/{schemaType}_{partId:d10}.parquet";
        }

        private JObject ParseResource(string content, string blobName, string eTag, long index)
        {
            try
            {
                var resource = JObject.Parse(content);
                resource.Add(DataLakeConstants.BlobNameColumnKey, new JValue(blobName));
                resource.Add(DataLakeConstants.ETagColumnKey, new JValue(eTag));
                resource.Add(DataLakeConstants.IndexColumnKey, new JValue(index));

                return resource;
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError("The input content is invalid JSON.");
                _logger.LogInformation("The input content is invalid JSON.");
                throw new JobExecutionException("The input content is invalid JSON.", ex);
            }
        }
    }
}