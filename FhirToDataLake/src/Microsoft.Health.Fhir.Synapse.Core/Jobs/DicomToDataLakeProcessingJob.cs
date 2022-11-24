// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Dicom.Client.Models;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Dicom;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api.Dicom;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.DicomApiOption;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class DicomToDataLakeProcessingJob : IJob
    {
        private readonly DicomToDataLakeProcessingJobInputData _inputData;
        private readonly IApiDataClient _dataClient;
        private readonly IDataWriter _dataWriter;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly ISchemaManager<ParquetSchemaNode> _schemaManager;
        private readonly IMetricsLogger _metricsLogger;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<DicomToDataLakeProcessingJob> _logger;

        private readonly long _jobId;

        private FhirToDataLakeProcessingJobResult _result;
        private CacheResult _cacheResult;

        /// <summary>
        /// Output file index map for all resources/schemas.
        /// The value of each schemaType will be appended to output files.
        /// The format is '{SchemaType}_{index:d10}.parquet', e.g. Patient_0000000001.parquet.
        /// </summary>
        private Dictionary<string, int> _outputFileIndexMap;

        public DicomToDataLakeProcessingJob(
            long jobId,
            DicomToDataLakeProcessingJobInputData inputData,
            IApiDataClient dataClient,
            IDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            ISchemaManager<ParquetSchemaNode> schemaManager,
            IMetricsLogger metricsLogger,
            IDiagnosticLogger diagnosticLogger,
            ILogger<DicomToDataLakeProcessingJob> logger)
        {
            _jobId = jobId;
            _inputData = EnsureArg.IsNotNull(inputData, nameof(inputData));

            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _schemaManager = EnsureArg.IsNotNull(schemaManager, nameof(schemaManager));
            _metricsLogger = EnsureArg.IsNotNull(metricsLogger, nameof(metricsLogger));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        // the processing job status is never set to failed or cancelled.
        public async Task<string> ExecuteAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Start executing processing job {_jobId}.");

            try
            {
                // clear result at first
                _result = new FhirToDataLakeProcessingJobResult();

                progress.Report(JsonConvert.SerializeObject(_result));

                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Job is cancelled.");
                    throw new OperationCanceledException();
                }

                // clean resources before process start
                await CleanResourceAsync(cancellationToken);

                // Initialize
                _cacheResult = new CacheResult();
                if (!_cacheResult.Resources.ContainsKey(DicomConstants.DicomResourceType))
                {
                    _cacheResult.Resources[DicomConstants.DicomResourceType] = new List<JObject>();
                }

                _outputFileIndexMap = new Dictionary<string, int>();

                // Search metadata
                await SearchAsync(progress, cancellationToken);

                // force to commit result when all the resources of this job are processed.
                await TryCommitResultAsync(progress, true, cancellationToken);

                _result.ProcessingCompleteTime = DateTime.UtcNow;

                progress.Report(JsonConvert.SerializeObject(_result));

                _logger.LogInformation($"Finished processing job '{_jobId}'.");

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
                // always throw RetriableJobException
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
            catch (Exception ex)
            {
                // Unhandled exceptions.
                _logger.LogError(ex, "Unhandled error occurred in data processing job {0}. Reason : {1}", _jobId, ex.Message);
                _metricsLogger.LogTotalErrorsMetrics(ex, $"Unhandled error occurred in data processing job. Reason: {ex.Message}", JobOperations.RunJob);
                await CleanResourceAsync(CancellationToken.None);

                throw new RetriableJobException("Unhandled error occurred in data processing job.", ex);
            }
        }

        private async Task SearchAsync(
            IProgress<string> progress,
            CancellationToken cancellationToken)
        {
            var limit = _inputData.EndOffset - _inputData.StartOffset;

            if (limit > 0)
            {
                // Get metadata objects from change feeds
                SearchResult metadataObjectsResult = await GetMetadataObjectsAsync(limit, cancellationToken);

                foreach (JObject metadataObject in metadataObjectsResult.Resources)
                {
                    _cacheResult.Resources[DicomConstants.DicomResourceType].Add(metadataObject);
                }

                _cacheResult.CacheSize += metadataObjectsResult.ResultSizeInBytes;
                await TryCommitResultAsync(progress, false, cancellationToken);
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
                        StreamBatchData parquetStream = await _parquetDataProcessor.ProcessAsync(batchData, processParameters, cancellationToken);
                        int skippedCount = batchData.Values.Count() - parquetStream.BatchSize;

                        if (parquetStream.Value?.Length > 0)
                        {
                            if (!_outputFileIndexMap.ContainsKey(schemaType))
                            {
                                _outputFileIndexMap[schemaType] = 0;
                            }

                            // Upload to blob and log result
                            var fileName = GetDataFileName(_inputData.EndOffset, schemaType, _jobId, _outputFileIndexMap[schemaType]);
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

        private async Task<SearchResult> GetMetadataObjectsAsync(long limit, CancellationToken cancellationToken)
        {
            var queryParameters = new List<KeyValuePair<string, string>>
                {
                    new KeyValuePair<string, string>(DicomApiConstants.OffsetKey, $"{_inputData.StartOffset}"),
                    new KeyValuePair<string, string>(DicomApiConstants.LimitKey, $"{limit}"),
                    new KeyValuePair<string, string>(DicomApiConstants.IncludeMetadataKey, $"{true}"),
                };

            var changeFeedOptions = new ChangeFeedOffsetOptions(queryParameters);
            string changeFeedsContent = await _dataClient.SearchAsync(changeFeedOptions, cancellationToken);

            List<JObject> metadataObjects;
            try
            {
                metadataObjects = JArray.Parse(changeFeedsContent)
                    .Where(changeFeed =>
                        Enum.Parse<ChangeFeedState>(changeFeed[DicomChangeFeedConstants.StateKey].ToString(), true) == ChangeFeedState.Current &&
                        Enum.Parse<ChangeFeedAction>(changeFeed[DicomChangeFeedConstants.ActionKey].ToString(), true) == ChangeFeedAction.Create)
                    .Select(changeFeed =>
                    {
                        var metadataObject = changeFeed[DicomChangeFeedConstants.MetadataKey] as JObject;

                        metadataObject.Add(DicomConstants.SequenceColumnKey, changeFeed[DicomChangeFeedConstants.SequenceKey].ToString());
                        metadataObject.Add(DicomConstants.TimestampColumnKey, changeFeed[DicomChangeFeedConstants.TimestampKey].ToString());
                        return metadataObject;
                    }).ToList();
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Parse metadata objects failed. Reason: {ex.Message}");
                _logger.LogError($"Parse metadata objects failed. Reason: {ex.Message}");
                throw new ApiSearchException("Parse metadata objects failed.", ex);
            }

            return new SearchResult(metadataObjects, changeFeedsContent.Length, null);
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
            long offset,
            string schemaType,
            long jobId,
            int partId)
        {
            return $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}/{schemaType}/{offset}/{schemaType}_{partId:d10}.parquet";
        }
    }
}