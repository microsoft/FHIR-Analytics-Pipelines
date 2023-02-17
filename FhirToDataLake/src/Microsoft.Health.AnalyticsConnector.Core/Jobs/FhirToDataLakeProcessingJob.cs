// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
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
using Microsoft.Health.AnalyticsConnector.Common.Models.FhirSearch;
using Microsoft.Health.AnalyticsConnector.Common.Models.Jobs;
using Microsoft.Health.AnalyticsConnector.Core.DataFilter;
using Microsoft.Health.AnalyticsConnector.Core.DataProcessor;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.Core.Extensions;
using Microsoft.Health.AnalyticsConnector.Core.Fhir;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Fhir;
using Microsoft.Health.AnalyticsConnector.DataClient.Exceptions;
using Microsoft.Health.AnalyticsConnector.DataClient.Extensions;
using Microsoft.Health.AnalyticsConnector.DataClient.Models.FhirApiOption;
using Microsoft.Health.AnalyticsConnector.DataWriter;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public class FhirToDataLakeProcessingJob : IJob
    {
        private readonly FhirToDataLakeProcessingJobInputData _inputData;
        private readonly IApiDataClient _dataClient;
        private readonly IDataWriter _dataWriter;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly ISchemaManager<ParquetSchemaNode> _schemaManager;
        private readonly IGroupMemberExtractor _groupMemberExtractor;
        private readonly IFilterManager _filterManager;
        private readonly IMetricsLogger _metricsLogger;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<FhirToDataLakeProcessingJob> _logger;

        private readonly long _jobId;

        private FhirToDataLakeProcessingJobResult _result;
        private List<TypeFilter> _typeFilters;
        private CacheResult _cacheResult;
        private FilterScope _filterScope;

        /// <summary>
        /// Output file index map for all resources/schemas.
        /// The value of each schemaType will be appended to output files.
        /// The format is '{SchemaType}_{index:d10}.parquet', e.g. Patient_0000000001.parquet.
        /// </summary>
        private Dictionary<string, int> _outputFileIndexMap;

        // Date format in blob path.
        private const string DateKeyFormat = "yyyy/MM/dd";

        public FhirToDataLakeProcessingJob(
            long jobId,
            FhirToDataLakeProcessingJobInputData inputData,
            IApiDataClient dataClient,
            IDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            ISchemaManager<ParquetSchemaNode> schemaManager,
            IGroupMemberExtractor groupMemberExtractor,
            IFilterManager filterManager,
            IMetricsLogger metricsLogger,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirToDataLakeProcessingJob> logger)
        {
            _jobId = jobId;
            _inputData = EnsureArg.IsNotNull(inputData, nameof(inputData));

            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _schemaManager = EnsureArg.IsNotNull(schemaManager, nameof(schemaManager));
            _groupMemberExtractor = EnsureArg.IsNotNull(groupMemberExtractor, nameof(groupMemberExtractor));
            _filterManager = EnsureArg.IsNotNull(filterManager, nameof(filterManager));
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

                _cacheResult = new CacheResult();
                _outputFileIndexMap = new Dictionary<string, int>();

                _typeFilters = await _filterManager.GetTypeFiltersAsync(cancellationToken);

                _filterScope = await _filterManager.GetFilterScopeAsync(cancellationToken);
                switch (_filterScope)
                {
                    case FilterScope.Group:
                    {
                        await GroupExecuteAsyncInternal(progress, cancellationToken);
                        break;
                    }

                    case FilterScope.System:
                    {
                        await SystemExecuteAsyncInternal(progress, cancellationToken);
                        break;
                    }

                    default:
                        throw new ConfigurationErrorException(
                            $"The FilterScope {_filterScope} isn't supported now.");
                }

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
            finally
            {
                // Compact large objects allocated in processing.
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect();
            }
        }

        private async Task SystemExecuteAsyncInternal(IProgress<string> progress, CancellationToken cancellationToken)
        {
            // create initial base search option for this task,
            // the resource type and customized parameters of each filter will be set later.
            List<KeyValuePair<string, string>> parameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Batch.ToString("d")),
            };

            var searchOption = new BaseSearchOptions(null, parameters);

            // retrieve resources for all the type filters.
            await ProcessFiltersAsync(progress, searchOption, cancellationToken);
        }

        private async Task GroupExecuteAsyncInternal(IProgress<string> progress, CancellationToken cancellationToken)
        {
            bool isPatientResourcesRequired = IsPatientResourcesRequired(_typeFilters);

            // TODO: how to ensure the group is the same?
            HashSet<string> allPatientIds = await _groupMemberExtractor.GetGroupPatientsAsync(
                await _filterManager.GetGroupIdAsync(cancellationToken),
                null,
                _inputData.DataEndTime,
                cancellationToken);

            Dictionary<string, string> patientHashToId = allPatientIds.ToDictionary(
                TableKeyProvider.CompartmentRowKey,
                patientId => patientId);
            foreach (PatientWrapper patientInfo in _inputData.ToBeProcessedPatients)
            {
                long lastPatientVersionId = patientInfo.VersionId;

                if (!patientHashToId.ContainsKey(patientInfo.PatientHash))
                {
                    _logger.LogInformation($"Can't find patient in group for patient hash {patientInfo.PatientHash}, the group is modified.");
                    continue;
                }

                string patientId = patientHashToId[patientInfo.PatientHash];

                // the patient resource isn't included in compartment search,
                // so we need additional request to get the patient resource
                JObject patientResource = await GetPatientResource(patientId, cancellationToken);

                // the patient does not exist, skip processing this patient
                if (patientResource == null)
                {
                    continue;
                }

                int currentPatientVersionId = FhirBundleParser.ExtractVersionId(patientResource);

                if (currentPatientVersionId == 0)
                {
                    _diagnosticLogger.LogError(
                        $"Failed to extract version id for patient {patientId}.");
                    _logger.LogInformation(
                        $"Failed to extract version id for patient {patientId}.");
                    throw new ApiSearchException(
                        $"Failed to extract version id for patient {patientId}.");
                }

                // New patient or the patient is updated.
                if (lastPatientVersionId != currentPatientVersionId)
                {
                    // save the patient resource to cache if the patient resource type is required in the result
                    if (isPatientResourcesRequired)
                    {
                        AddFhirResourcesToCache(new List<JObject> { patientResource });
                    }
                }

                // add this patient's version id in result
                _result.ProcessedPatientVersion[patientInfo.PatientHash] = currentPatientVersionId;

                _logger.LogInformation($"Get patient resource {patientId} successfully.");

                // the version id is 0 for newly patient
                // for new patient, we will retrieve all its compartments resources from {since}
                // for processed patient, we will only retrieve the updated compartment resources from last scheduled time
                DateTimeOffset? startDateTime = lastPatientVersionId == 0
                    ? _inputData.Since
                    : _inputData.DataStartTime;
                List<KeyValuePair<string, string>> parameters = new List<KeyValuePair<string, string>>
                            {
                                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{_inputData.DataEndTime.ToInstantString()}"),
                                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Batch.ToString("d")),
                            };

                if (startDateTime != null)
                {
                    parameters.Add(new KeyValuePair<string, string>(
                        FhirApiConstants.LastUpdatedKey,
                        $"ge{((DateTimeOffset)startDateTime).ToInstantString()}"));
                }

                // create initial compartment search option for this patient,
                // the resource type and customized parameters of each filter will be set later.
                var searchOption = new CompartmentSearchOptions(
                    FhirConstants.PatientResource,
                    patientId,
                    null,
                    parameters);

                // retrieve this patient's compartment resources for all the filters
                await ProcessFiltersAsync(progress, searchOption, cancellationToken);

                _logger.LogInformation($"Process patient resource {patientId} successfully.");
            }
        }

        /// <summary>
        /// If we need retrieve patient resources for group filter scope
        /// </summary>
        private static bool IsPatientResourcesRequired(IEnumerable<TypeFilter> typeFilters)
        {
            foreach (TypeFilter typeFilter in typeFilters)
            {
                switch (typeFilter.ResourceType)
                {
                    // if patient resource is required in typeFilter
                    case FhirConstants.PatientResource:
                        return true;
                    case FhirConstants.AllResource:
                    {
                        // all resources are required without any parameters
                        if (typeFilter.Parameters == null || !typeFilter.Parameters.Any())
                        {
                            return true;
                        }

                        foreach ((string param, string value) in typeFilter.Parameters)
                        {
                            if (param != FhirApiConstants.TypeKey)
                            {
                                continue;
                            }

                            // if patient is in _type parameter
                            string[] types = value.Split(',');
                            if (types.Contains(FhirConstants.PatientResource))
                            {
                                return true;
                            }
                        }

                        break;
                    }
                }
            }

            return false;
        }

        private async Task<JObject> GetPatientResource(string patientId, CancellationToken cancellationToken)
        {
            var patientSearchOption = new ResourceIdSearchOptions(
                FhirConstants.PatientResource,
                patientId,
                null);

            SearchResult searchResult = await ExecuteSearchAsync(patientSearchOption, cancellationToken);

            // if the patient does not exist, log a warning, and do nothing about it.
            if (searchResult.Resources == null || !searchResult.Resources.Any())
            {
                _logger.LogInformation($"The patient {patientId} dose not exist in fhir server, ignore it.");
                return null;
            }

            JObject patientResource = searchResult.Resources[0];

            if (patientResource["resourceType"]?.ToString() != FhirConstants.PatientResource)
            {
                _diagnosticLogger.LogError($"Failed to get patient {patientId}.");
                _logger.LogInformation($"Failed to get patient {patientId}.");
                throw new ApiSearchException($"Failed to get patient {patientId}.");
            }

            return patientResource;
        }

        /// <summary>
        /// Get resources for all the type filters. The resources are stored in cache result.
        /// </summary>
        private async Task ProcessFiltersAsync(
            IProgress<string> progress,
            BaseSearchOptions searchOptions,
            CancellationToken cancellationToken)
        {
            List<KeyValuePair<string, string>> sharedQueryParameters = new List<KeyValuePair<string, string>>(searchOptions.QueryParameters);

            foreach (TypeFilter typeFilter in _typeFilters)
            {
                searchOptions.QueryParameters = new List<KeyValuePair<string, string>>(sharedQueryParameters);

                if (_filterScope == FilterScope.System)
                {
                    if (_inputData.JobVersion >= JobVersion.V4)
                    {
                        if (!_inputData.SplitParameters.ContainsKey(typeFilter.ResourceType))
                        {
                            continue;
                        }

                        searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(
                                FhirApiConstants.LastUpdatedKey,
                                $"lt{_inputData.SplitParameters[typeFilter.ResourceType].DataEndTime.ToInstantString()}"));

                        if (_inputData.SplitParameters[typeFilter.ResourceType].DataStartTime != null)
                        {
                            searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(
                                FhirApiConstants.LastUpdatedKey,
                                $"ge{((DateTimeOffset)_inputData.SplitParameters[typeFilter.ResourceType].DataStartTime).ToInstantString()}"));
                        }
                    }
                    else
                    {
                        searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(
                            FhirApiConstants.LastUpdatedKey,
                            $"lt{_inputData.DataEndTime.ToInstantString()}"));
                        if (_inputData.DataStartTime != null)
                        {
                            searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(
                                FhirApiConstants.LastUpdatedKey,
                                $"ge{((DateTimeOffset)_inputData.DataStartTime).ToInstantString()}"));
                        }
                    }
                }

                searchOptions.ResourceType = typeFilter.ResourceType;
                foreach (KeyValuePair<string, string> parameter in typeFilter.Parameters)
                {
                    searchOptions.QueryParameters.Add(parameter);
                }

                await SearchWithFilterAsync(progress, searchOptions, cancellationToken);
            }
        }

        /// <summary>
        /// Get fhir resources with the searchOption specified.
        /// If there is any operationOutcomes, will throw an exception
        /// </summary>
        private async Task SearchWithFilterAsync(
            IProgress<string> progress,
            BaseSearchOptions searchOptions,
            CancellationToken cancellationToken)
        {
            bool isCurrentSearchCompleted = false;
            string continuationToken = null;

            while (!isCurrentSearchCompleted)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                // add/update continuation token
                if (continuationToken != null)
                {
                    bool replacedContinuationToken = false;

                    // if continuation token parameter exists, update it
                    for (int index = 0; index < searchOptions.QueryParameters.Count; index++)
                    {
                        if (searchOptions.QueryParameters[index].Key != FhirApiConstants.ContinuationKey)
                        {
                            continue;
                        }

                        searchOptions.QueryParameters[index] = new KeyValuePair<string, string>(
                            FhirApiConstants.ContinuationKey, continuationToken);
                        replacedContinuationToken = true;
                        break;
                    }

                    // if continuation token isn't set before, add it
                    if (!replacedContinuationToken)
                    {
                        searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, continuationToken));
                    }
                }

                SearchResult searchResult = await ExecuteSearchAsync(searchOptions, cancellationToken);

                // add resources to memory cache
                AddFhirResourcesToCache(searchResult.Resources);
                _cacheResult.CacheSize += searchResult.ResultSizeInBytes;

                continuationToken = searchResult.ContinuationToken;

                if (string.IsNullOrWhiteSpace(continuationToken))
                {
                    isCurrentSearchCompleted = true;
                }

                // check if necessary to commit the resources in memory cache to storage
                await TryCommitResultAsync(progress, false, cancellationToken);
            }
        }

        /// <summary>
        /// Execute search, extract resources and continuation token from the response.
        /// An Exception will be thrown if there are operationOutcomes
        /// </summary>
        private async Task<SearchResult> ExecuteSearchAsync(BaseSearchOptions searchOptions, CancellationToken cancellationToken)
        {
            string fhirBundleResult = await _dataClient.SearchAsync(searchOptions, cancellationToken);

            // Parse bundle result.
            JObject fhirBundleObject;
            try
            {
                fhirBundleObject = JObject.Parse(fhirBundleResult);
            }
            catch (Exception exception)
            {
                _diagnosticLogger.LogError("Failed to parse fhir search result.");
                _logger.LogInformation(exception, "Failed to parse fhir search result.");
                throw new FhirDataParseException("Failed to parse fhir search result", exception);
            }

            IEnumerable<JObject> fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject);

            List<JObject> operationOutcomes = FhirBundleParser.GetOperationOutcomes(fhirResources).ToList();
            if (operationOutcomes.Any())
            {
                _diagnosticLogger.LogError($"There is operationOutcome returned from FHIR server: {string.Join(',', operationOutcomes)}");
                _logger.LogInformation($"There is operationOutcome returned from FHIR server: {string.Join(',', operationOutcomes)}");
                throw new ApiSearchException($"There is operationOutcome returned from FHIR server: {string.Join(',', operationOutcomes)}");
            }

            string continuationToken = FhirBundleParser.ExtractContinuationToken(fhirBundleObject);

            return new SearchResult(fhirResources.ToList(), fhirBundleResult.Length * sizeof(char), continuationToken);
        }

        /// <summary>
        /// Add the resources extracted from a request to memory cache.
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
                            var dateTime = _inputData.SplitParameters != null && _inputData.SplitParameters.ContainsKey(resourceType) ? _inputData.SplitParameters[resourceType].DataEndTime : _inputData.DataEndTime;
                            var fileName = GetDataFileName(dateTime, schemaType, _jobId, _outputFileIndexMap[schemaType]);
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
            DateTimeOffset dateTime,
            string schemaType,
            long jobId,
            int partId)
        {
            string dateTimeKey = dateTime.ToString(DateKeyFormat);

            return $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}/{schemaType}/{dateTimeKey}/{schemaType}_{partId:d10}.parquet";
        }
    }
}