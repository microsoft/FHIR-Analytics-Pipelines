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
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Extensions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class FhirToDataLakeProcessingJob : IJob
    {
        private JobInfo _jobInfo;
        private readonly FhirToDataLakeProcessingJobInputData _inputData;
        private FhirToDataLakeProcessingJobResult _result;

        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly IFhirSchemaManager<FhirParquetSchemaNode> _fhirSchemaManager;
        private readonly ILogger<FhirToDataLakeProcessingJob> _logger;

        private CacheResult _cacheResult;

        /// <summary>
        /// Output file index map for all resources/schemas.
        /// The value of each schemaType will be appended to output files.
        /// The format is '{SchemaType}_{TaskIndex:d10}_{index:d10}.parquet', e.g. Patient_0000000001_0000000001.parquet.
        /// </summary>
        private Dictionary<string, int> _outputFileIndexMap;

        public FhirToDataLakeProcessingJob(
            JobInfo jobInfo,
            FhirToDataLakeProcessingJobInputData inputData,
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            ILogger<FhirToDataLakeProcessingJob> logger)
        {
            _jobInfo = EnsureArg.IsNotNull(jobInfo, nameof(jobInfo));
            _inputData = EnsureArg.IsNotNull(inputData, nameof(inputData));

            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _fhirSchemaManager = EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            // TODO: need to clear result in jobinfo
            // TODO: delete stage folder
            // TODO: clean result in jobEntity
            // the processing job status is never set to failed or cancelled.
            _result = new FhirToDataLakeProcessingJobResult();
            _jobInfo.Result = string.Empty;

            _cacheResult = new CacheResult();
            _outputFileIndexMap = new Dictionary<string, int>();
        }

        // the main progress:
        // 1. The retrieved fhir resources and its search progress are in memory cache temporarily;
        // 2. the cached resources will be committed to blob through "TryCommitResultAsync()" function when the number of cached resources reaches the specified value or the task is finished,
        //    the fields of task context (statistical fields and searchProgress) are updated concurrently;
        // 3. when the task is completed, set the task status to completed
        // 4. once the task context is updated either from step 2 or step 3, calls "JobProgressUpdater.Produce()" to sync task context to job
        // 5. "JobProgressUpdater.Consume" will handle the updated task context,
        //    when the task is completed, add statistical fields and patient version id to job and remove it from runningTasks;
        // 6. call "_jobStore.UpdateJobAsync()" to save the job context to storage in "JobProgressUpdater.Consume()" at regular intervals or when completing producing task context 
        public async Task<string> ExecuteAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation($"Job is cancelled.");
                throw new OperationCanceledException();
            }

            _result.ProcessingStartTime = DateTime.Now;

            _logger.LogInformation($"Start executing processing job {_inputData.ProcessingJobSequenceId}.");
            try
            {
                switch (_inputData.FilterScope)
                {
                    case FilterScope.Group:
                    {
                        var isPatientResourcesRequired = IsPatientResourcesRequired(_inputData.TypeFilters);

                        foreach (var patientInfo in ((CompartmentProcessingInputMetadata) _inputData.InputMetadata)
                                 .ToBeProcessedPatients)
                        {
                            var lastPatientVersionId = patientInfo.VersionId;

                            // the patient resource isn't included in compartment search,
                            // so we need additional request to get the patient resource

                            // the patient resource is not retrieved yet,
                            if (!((CompartmentResultMetadata) _result.ResultMetadata).ProcessedPatientVersions
                                .ContainsKey(patientInfo.PatientId))
                            {
                                var patientResource = await GetPatientResource(patientInfo, cancellationToken);

                                // the patient does not exist, skip processing this patient
                                if (patientResource == null)
                                {
                                    continue;
                                }

                                var currentPatientVersionId = FhirBundleParser.ExtractVersionId(patientResource);

                                if (currentPatientVersionId == 0)
                                {
                                    _logger.LogError(
                                        $"Failed to extract version id for patient {patientInfo.PatientId}.");
                                    throw new FhirSearchException(
                                        $"Failed to extract version id for patient {patientInfo.PatientId}.");
                                }

                                // New patient or the patient is updated.
                                if (lastPatientVersionId != currentPatientVersionId)
                                {
                                    // save the patient resource to cache if the patient resource type is required in the result
                                    if (isPatientResourcesRequired)
                                    {
                                        AddFhirResourcesToCache(new List<JObject> {patientResource});
                                    }
                                }

                                // add this patient's version id in cacheResult,
                                // the version id will be synced to taskContext when the cache result is committed, and be recorded in job/schedule metadata further
                                ((CompartmentResultMetadata) _result.ResultMetadata).ProcessedPatientVersions[
                                    patientInfo.PatientId] = currentPatientVersionId;
                                _logger.LogInformation($"Get patient resource {patientInfo.PatientId} successfully.");
                            }

                            // the version id is 0 for newly patient
                            // for new patient, we will retrieve all its compartments resources from {since}
                            // for processed patient, we will only retrieve the updated compartment resources from last scheduled time
                            var startDateTime = lastPatientVersionId == 0
                                ? _inputData.Since
                                : _inputData.DataStartTime;
                            var parameters = new List<KeyValuePair<string, string>>
                            {
                                new(FhirApiConstants.LastUpdatedKey, $"ge{startDateTime.ToInstantString()}"),
                                new(FhirApiConstants.LastUpdatedKey, $"lt{_inputData.DataEndTime.ToInstantString()}"),
                            };

                            // create initial compartment search option for this patient,
                            // the resource type and customized parameters of each filter will be set later.
                            var searchOption = new CompartmentSearchOptions(
                                FhirConstants.PatientResource,
                                patientInfo.PatientId,
                                null,
                                parameters);

                            // retrieve this patient's compartment resources for all the filters
                            await ProcessFiltersAsync(progress, searchOption, cancellationToken);

                            _logger.LogInformation($"Process patient resource {patientInfo.PatientId} successfully.");
                        }

                        break;
                    }

                    case FilterScope.System:
                    {
                        // create initial base search option for this task,
                        // the resource type and customized parameters of each filter will be set later.
                        var parameters = new List<KeyValuePair<string, string>>
                        {
                            new(FhirApiConstants.LastUpdatedKey, $"ge{_inputData.DataStartTime.ToInstantString()}"),
                            new(FhirApiConstants.LastUpdatedKey, $"lt{_inputData.DataEndTime.ToInstantString()}"),
                        };
                        var searchOption = new BaseSearchOptions(null, parameters);

                        // retrieve resources for all the type filters.
                        await ProcessFiltersAsync(progress, searchOption, cancellationToken);
                        break;
                    }

                    default:
                        throw new ArgumentOutOfRangeException(
                            $"The FilterScope {_inputData.FilterScope} isn't supported now.");
                }
            }
            catch (RetriableJobException e)
            {
                // TODO: try to delete stage folder
                throw;
            }
            catch (Exception ex)
            {
                // TODO: try to delete stage folder
                throw new JobExecutionException(ex.Message);
            }

            // force to commit result when all the resources of this task are processed.
            await TryCommitResultAsync(progress, true, cancellationToken);

            _result.ProcessingCompleteTime = DateTime.UtcNow;

            progress.Report(JsonConvert.SerializeObject(_result));

            _logger.LogInformation($"Finished processing job '{_inputData.ProcessingJobSequenceId}'.");

            return JsonConvert.SerializeObject(_result);

            // TODO:
            // 1. job completed
            // 2. throw retirableJobExcep
        }

        /// <summary>
        /// If we need retrieve patient resources for group filter scope
        /// </summary>
        private static bool IsPatientResourcesRequired(IEnumerable<TypeFilter> typeFilters)
        {
            foreach (var typeFilter in typeFilters)
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

                        foreach (var (param, value) in typeFilter.Parameters)
                        {
                            if (param != FhirApiConstants.TypeKey)
                            {
                                continue;
                            }

                            // if patient is in _type parameter
                            var types = value.Split(',');
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

        private async Task<JObject> GetPatientResource(PatientWrapper patientInfo, CancellationToken cancellationToken)
        {
            var patientSearchOption = new ResourceIdSearchOptions(
                FhirConstants.PatientResource,
                patientInfo.PatientId,
                null);

            var searchResult = await ExecuteSearchAsync(patientSearchOption, cancellationToken);

            // if the patient does not exist, log a warning, and do nothing about it.
            if (searchResult.FhirResources == null || !searchResult.FhirResources.Any())
            {
                _logger.LogWarning($"The patient {patientInfo.PatientId} dose not exist in fhir server, ignore it.");
                return null;
            }

            var patientResource = searchResult.FhirResources[0];

            if (patientResource["resourceType"]?.ToString() != FhirConstants.PatientResource)
            {
                _logger.LogError($"Failed to get patient {patientInfo.PatientId}.");
                throw new FhirSearchException($"Failed to get patient {patientInfo.PatientId}.");
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
            var sharedQueryParameters = new List<KeyValuePair<string, string>>(searchOptions.QueryParameters);

            foreach (var typeFilter in _inputData.TypeFilters)
            {
                searchOptions.ResourceType = typeFilter.ResourceType;
                searchOptions.QueryParameters = new List<KeyValuePair<string, string>>(sharedQueryParameters);
                foreach (var parameter in typeFilter.Parameters)
                {
                    searchOptions.QueryParameters.Add(parameter);
                }

                await SearchWithFilterAsync(progress, searchOptions, cancellationToken);
            }
        }

        /// <summary>
        /// Get fhir resources with the searchOption specified.
        /// the continuation token is updated for taskContext after each request, marked as IsCompleted if there is no continuation token anymore.
        /// If there is any operationOutcomes, will throw an exception
        /// </summary>
        private async Task SearchWithFilterAsync(
            IProgress<string> progress,
            BaseSearchOptions searchOptions,
            CancellationToken cancellationToken)
        {
            var isCurrentSearchCompleted = false;
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
                    var replacedContinuationToken = false;

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

                var searchResult = await ExecuteSearchAsync(searchOptions, cancellationToken);

                // add resources to memory cache
                AddFhirResourcesToCache(searchResult.FhirResources);
                _cacheResult.CacheSize += searchResult.ResultSizeInBytes;

                continuationToken = searchResult.ContinuationToken;

                if (string.IsNullOrEmpty(continuationToken))
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
            var fhirBundleResult = await _dataClient.SearchAsync(searchOptions, cancellationToken);

            // Parse bundle result.
            JObject fhirBundleObject;
            try
            {
                fhirBundleObject = JObject.Parse(fhirBundleResult);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Failed to parse fhir search result.");
                throw new FhirDataParseExeption($"Failed to parse fhir search result", exception);
            }

            var fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject);

            var operationOutcomes = FhirBundleParser.GetOperationOutcomes(fhirResources).ToList();
            if (operationOutcomes.Any())
            {
                _logger.LogError($"There are operationOutcomes returned from FHIR server: {string.Join(',', operationOutcomes)}");
                throw new FhirSearchException($"There is operationOutcome returned from FHIR server: {string.Join(',', operationOutcomes)}");
            }

            var continuationToken = FhirBundleParser.ExtractContinuationToken(fhirBundleObject);

            return new SearchResult(fhirResources.ToList(), fhirBundleResult.Length * sizeof(char), continuationToken);
        }

        /// <summary>
        /// Add the resources extracted from a request to memory cache.
        /// </summary>
        private void AddFhirResourcesToCache(List<JObject> fhirResources)
        {
            foreach (var resource in fhirResources)
            {
                var resourceType = resource["resourceType"]?.ToString();
                if (string.IsNullOrEmpty(resourceType))
                {
                    _logger.LogError($"Failed to parse fhir search resource {resource}");
                    throw new FhirDataParseExeption($"Failed to parse fhir search resource {resource}");
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
            var cacheResourceCount = _cacheResult.GetResourceCount();
            if (cacheResourceCount >= JobConfigurationConstants.NumberOfResourcesPerCommit ||
                _cacheResult.CacheSize > JobConfigurationConstants.DataSizeInBytesPerCommit ||
                forceCommit)
            {
                _logger.LogInformation($"commit data: {cacheResourceCount} resources, {_cacheResult.CacheSize} data size.");

                foreach (var (resourceType, resources) in _cacheResult.Resources)
                {
                    var batchData = new JsonBatchData(resources);

                    var schemaTypes = _fhirSchemaManager.GetSchemaTypes(resourceType);
                    foreach (var schemaType in schemaTypes)
                    {
                        // Convert grouped data to parquet stream
                        var processParameters = new ProcessParameters(schemaType);
                        var parquetStream = await _parquetDataProcessor.ProcessAsync(batchData, processParameters, cancellationToken);
                        var skippedCount = batchData.Values.Count() - parquetStream.BatchSize;

                        if (parquetStream?.Value?.Length > 0)
                        {
                            if (!_outputFileIndexMap.ContainsKey(schemaType))
                            {
                                _outputFileIndexMap[schemaType] = 0;
                            }

                            // Upload to blob and log result
                            var blobUrl = await _dataWriter.WriteAsync(parquetStream, $"{_inputData.TriggerSequenceId:d10}", (int)_inputData.ProcessingJobSequenceId, _outputFileIndexMap[schemaType], _inputData.DataEndTime, cancellationToken);
                            _outputFileIndexMap[schemaType] += 1;

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
                            _logger.LogWarning(
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

                // clear cache
                _cacheResult.ClearCache();
                progress.Report(JsonConvert.SerializeObject(_result));

                _logger.LogInformation($"Commit cache resources successfully for processing job {_inputData.ProcessingJobSequenceId}.");
            }
        }

    }
}
