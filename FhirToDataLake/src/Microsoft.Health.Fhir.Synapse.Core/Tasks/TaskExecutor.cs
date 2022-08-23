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
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Extensions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Tasks
{
    public sealed class TaskExecutor : ITaskExecutor
    {
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly IFhirSchemaManager<FhirParquetSchemaNode> _fhirSchemaManager;
        private readonly ILogger<TaskExecutor> _logger;

        // TODO: Refine TaskExecutor here, current TaskExecutor is more like a manager class.
        public TaskExecutor(
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            ILogger<TaskExecutor> logger)
        {
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _dataWriter = EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            _parquetDataProcessor = EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            _fhirSchemaManager = EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        // the job/task main progress:
        // 1. The retrieved fhir resources and its search progress are in memory cache temporarily;
        // 2. the cached resources will be committed to blob through "TryCommitResultAsync()" function when the number of cached resources reaches the specified value or the task is finished,
        //    the fields of task context (statistical fields and searchProgress) are updated concurrently;
        // 3. when the task is completed, set the task status to completed
        // 4. once the task context is updated either from step 2 or step 3, calls "JobProgressUpdater.Produce()" to sync task context to job
        // 5. "JobProgressUpdater.Consume" will handle the updated task context,
        //    when the task is completed, add statistical fields and patient version id to job and remove it from runningTasks;
        // 6. call "_jobStore.UpdateJobAsync()" to save the job context to storage in "JobProgressUpdater.Consume()" at regular intervals or when completing producing task context.
        public async Task<TaskResult> ExecuteAsync(
            TaskContext taskContext,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation($"Task is cancelled.");
                throw new OperationCanceledException();
            }

            _logger.LogInformation($"Start execute task {taskContext.TaskIndex}.");

            // Initialize cache result from the search progress of task context
            var cacheResult = new CacheResult(taskContext.SearchProgress.Copy());

            switch (taskContext.FilterScope)
            {
                case FilterScope.Group:
                {
                    var isPatientResourcesRequired = IsPatientResourcesRequired(taskContext);

                    for (var patientIndex = cacheResult.SearchProgress.CurrentIndex; patientIndex < taskContext.Patients.Count; patientIndex++)
                    {
                        // start a new patient, reset all the search progress fields except currentIndex to initial value,
                        // otherwise, there are two possible cases:
                        // 1. this is a new task, the currentIndex is 0, in this case, all the search progress fields are initial values
                        // 2. this is a resumed task, continue with the recorded search progress
                        if (cacheResult.SearchProgress.CurrentIndex != patientIndex)
                        {
                            cacheResult.SearchProgress.UpdateCurrentIndex(patientIndex);
                        }

                        var patientInfo = taskContext.Patients[patientIndex];
                        var lastPatientVersionId = patientInfo.VersionId;

                        // the patient resource isn't included in compartment search,
                        // so we need additional request to get the patient resource

                        // the patient resource is not retrieved yet,
                        // for resumed task, the current patient may be retrieved and its version id exists.
                        if (!cacheResult.SearchProgress.PatientVersionId.ContainsKey(patientInfo.PatientId))
                        {
                            var patientResource = await GetPatientResource(patientInfo, cancellationToken);

                            // the patient does not exist
                            if (patientResource == null)
                            {
                                continue;
                            }

                            var currentPatientVersionId = FhirBundleParser.ExtractVersionId(patientResource);

                            if (currentPatientVersionId == 0)
                            {
                                _logger.LogError($"Failed to extract version id for patient {patientInfo.PatientId}.");
                                throw new FhirSearchException($"Failed to extract version id for patient {patientInfo.PatientId}.");
                            }

                            // New patient or the patient is updated.
                            if (lastPatientVersionId != currentPatientVersionId)
                            {
                                // save the patient resource to cache if the patient resource type is required in the result
                                if (isPatientResourcesRequired)
                                {
                                    AddFhirResourcesToCache(new List<JObject> { patientResource }, cacheResult);
                                }
                            }

                            // add this patient's version id in cacheResult,
                            // the version id will be synced to taskContext when the cache result is committed, and be recorded in job/schedule metadata further
                            cacheResult.SearchProgress.PatientVersionId[patientInfo.PatientId] = currentPatientVersionId;
                            _logger.LogInformation($"Get patient resource {patientInfo.PatientId} successfully.");
                        }

                        // the version id is 0 for newly patient
                        // for new patient, we will retrieve all its compartments resources from {since}
                        // for processed patient, we will only retrieve the updated compartment resources from last scheduled time
                        var startDateTime = lastPatientVersionId == 0
                            ? taskContext.Since
                            : taskContext.DataPeriod.Start;
                        var parameters = new List<KeyValuePair<string, string>>
                        {
                            new (FhirApiConstants.LastUpdatedKey, $"ge{startDateTime.ToInstantString()}"),
                            new (FhirApiConstants.LastUpdatedKey, $"lt{taskContext.DataPeriod.End.ToInstantString()}"),
                        };

                        // create initial compartment search option for this patient,
                        // the resource type and customized parameters of each filter will be set later.
                        var searchOption = new CompartmentSearchOptions(
                            FhirConstants.PatientResource,
                            patientInfo.PatientId,
                            null,
                            parameters);

                        // retrieve this patient's compartment resources for all the filters
                        await ProcessFiltersAsync(taskContext, searchOption, cacheResult, progressUpdater, cancellationToken);

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
                        new (FhirApiConstants.LastUpdatedKey, $"ge{taskContext.DataPeriod.Start.ToInstantString()}"),
                        new (FhirApiConstants.LastUpdatedKey, $"lt{taskContext.DataPeriod.End.ToInstantString()}"),
                    };
                    var searchOption = new BaseSearchOptions(null, parameters);

                    // retrieve resources for all the type filters.
                    await ProcessFiltersAsync(taskContext, searchOption, cacheResult, progressUpdater, cancellationToken);
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException($"The FilterScope {taskContext.FilterScope} isn't supported now.");
            }

            // force to commit result when all the resources of this task are processed.
            await TryCommitResultAsync(taskContext, true, cacheResult, progressUpdater, cancellationToken);

            taskContext.IsCompleted = true;

            // update the completed task context to job
            await progressUpdater.Produce(taskContext, cancellationToken);

            _logger.LogInformation(
                "Finished processing task '{taskIndex}'.",
                taskContext.TaskIndex);

            return TaskResult.CreateFromTaskContext(taskContext);
        }

        /// <summary>
        /// If we need retrieve patient resources for group filter scope
        /// </summary>
        private static bool IsPatientResourcesRequired(TaskContext taskContext)
        {
            foreach (var typeFilter in taskContext.TypeFilters)
            {
                switch (typeFilter.ResourceType)
                {
                    // if patient resource is required in typeFilter
                    case FhirConstants.PatientResource:
                        return true;
                    case FhirConstants.AllResource:
                        {
                            if (typeFilter.Parameters == null || !typeFilter.Parameters.Any())
                            {
                                return true;
                            }

                            foreach (var (param, value) in typeFilter.Parameters)
                            {
                                // if patient is in _type parameter
                                if (param == FhirApiConstants.TypeKey)
                                {
                                    var types = value.Split(',');
                                    if (types.Contains(FhirConstants.PatientResource))
                                    {
                                        return true;
                                    }
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
        /// <param name="taskContext">task context.</param>
        /// <param name="searchOptions">search options.</param>
        /// <param name="cacheResult">cache result.</param>
        /// <param name="progressUpdater">progress updater.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>task.</returns>
        private async Task ProcessFiltersAsync(
            TaskContext taskContext,
            BaseSearchOptions searchOptions,
            CacheResult cacheResult,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken)
        {
            var sharedQueryParameters = new List<KeyValuePair<string, string>>(searchOptions.QueryParameters);

            for (var i = cacheResult.SearchProgress.CurrentFilter; i < taskContext.TypeFilters.Count; i++)
            {
                searchOptions.ResourceType = taskContext.TypeFilters[i].ResourceType;
                searchOptions.QueryParameters = new List<KeyValuePair<string, string>>(sharedQueryParameters);
                foreach (var parameter in taskContext.TypeFilters[i].Parameters)
                {
                    searchOptions.QueryParameters.Add(parameter);
                }

                // reset the fields to start to process a new filter
                if (cacheResult.SearchProgress.CurrentFilter != i)
                {
                    cacheResult.SearchProgress.UpdateCurrentFilter(i);
                }

                await SearchWithFilterAsync(searchOptions, taskContext, cacheResult, progressUpdater, cancellationToken);
            }
        }

        /// <summary>
        /// Get fhir resources with the searchOption specified.
        /// the continuation token is updated for taskContext after each request, marked as IsCompleted if there is no continuation token anymore.
        /// If there is any operationOutcomes, will throw an exception
        /// </summary>
        private async Task SearchWithFilterAsync(
            BaseSearchOptions searchOptions,
            TaskContext taskContext,
            CacheResult cacheResult,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken)
        {
            while (!cacheResult.SearchProgress.IsCurrentSearchCompleted)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                // add/update continuation token
                if (cacheResult.SearchProgress.ContinuationToken != null)
                {
                    bool replacedContinuationToken = false;

                    // if continuation token parameter exists, update it
                    for (int index = 0; index < searchOptions.QueryParameters.Count; index++)
                    {
                        if (searchOptions.QueryParameters[index].Key == FhirApiConstants.ContinuationKey)
                        {
                            searchOptions.QueryParameters[index] = new KeyValuePair<string, string>(
                                FhirApiConstants.ContinuationKey, cacheResult.SearchProgress.ContinuationToken);
                            replacedContinuationToken = true;
                            break;
                        }
                    }

                    // if continuation token isn't set before, add it
                    if (!replacedContinuationToken)
                    {
                        searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, cacheResult.SearchProgress.ContinuationToken));
                    }
                }

                var searchResult = await ExecuteSearchAsync(searchOptions, cancellationToken);

                // add resources to memory cache
                AddFhirResourcesToCache(searchResult.FhirResources, cacheResult);
                cacheResult.CacheSize += searchResult.ResultSizeInBytes;

                cacheResult.SearchProgress.ContinuationToken = searchResult.ContinuationToken;

                if (string.IsNullOrEmpty(cacheResult.SearchProgress.ContinuationToken))
                {
                    cacheResult.SearchProgress.IsCurrentSearchCompleted = true;
                }

                // check if necessary to commit the resources in memory cache to storage and update search progress to task context
                await TryCommitResultAsync(taskContext, false, cacheResult, progressUpdater, cancellationToken);
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
        private void AddFhirResourcesToCache(List<JObject> fhirResources, CacheResult cacheResult)
        {
            foreach (var resource in fhirResources)
            {
                var resourceType = resource["resourceType"]?.ToString();
                if (string.IsNullOrEmpty(resourceType))
                {
                    _logger.LogError($"Failed to parse fhir search resource {resource}");
                    throw new FhirDataParseExeption($"Failed to parse fhir search resource {resource}");
                }

                if (!cacheResult.Resources.ContainsKey(resourceType))
                {
                    cacheResult.Resources[resourceType] = new List<JObject>();
                }

                cacheResult.Resources[resourceType].Add(resource);
            }
        }

        /// <summary>
        /// Try to commit the resources in memory cache to storage,
        /// if resources are committed,
        /// 1. search progress in cache is synced to task context, and update the task context to job.
        /// 2. clear the cache resources
        /// </summary>
        private async Task TryCommitResultAsync(
            TaskContext taskContext,
            bool forceCommit,
            CacheResult cacheResult,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken)
        {
            var cacheResourceCount = cacheResult.GetResourceCount();
            if (cacheResourceCount >= JobConfigurationConstants.NumberOfResourcesPerCommit ||
                cacheResult.CacheSize > JobConfigurationConstants.DataSizeInBytesPerCommit ||
                forceCommit)
            {
                _logger.LogInformation($"commit data: {cacheResourceCount} resources, {cacheResult.CacheSize} data size");

                foreach (var (resourceType, resources) in cacheResult.Resources)
                {
                    long outputDataSize = 0;
                    var inputData = new JsonBatchData(resources);

                    var schemaTypes = _fhirSchemaManager.GetSchemaTypes(resourceType);
                    foreach (var schemaType in schemaTypes)
                    {
                        // Convert grouped data to parquet stream
                        var processParameters = new ProcessParameters(schemaType, resourceType);
                        var parquetStream = await _parquetDataProcessor.ProcessAsync(inputData, processParameters, cancellationToken);
                        var skippedCount = inputData.Values.Count() - parquetStream.BatchSize;

                        if (parquetStream?.Value?.Length > 0)
                        {
                            outputDataSize += parquetStream.Value.Length;

                            if (!taskContext.OutputFileIndexMap.ContainsKey(schemaType))
                            {
                                taskContext.OutputFileIndexMap[schemaType] = 0;
                            }

                            // Upload to blob and log result
                            var blobUrl = await _dataWriter.WriteAsync(parquetStream, taskContext.JobId, taskContext.TaskIndex, taskContext.OutputFileIndexMap[schemaType], taskContext.DataPeriod.End, cancellationToken);
                            taskContext.OutputFileIndexMap[schemaType] += 1;

                            var batchResult = new BatchDataResult(
                                resourceType,
                                schemaType,
                                blobUrl,
                                inputData.Values.Count(),
                                inputData.Values.Count() - skippedCount);
                            _logger.LogInformation(
                                "{resourceCount} resources are searched in total. {processedCount} resources are processed. Detail: {detail}",
                                batchResult.ResourceCount,
                                batchResult.ProcessedCount,
                                batchResult.ToString());
                        }
                        else
                        {
                            _logger.LogWarning(
                                "No resource of schema type {schemaType} from {resourceType} is processed. {skippedCount} resources are skipped.",
                                schemaType,
                                resourceType,
                                skippedCount);
                        }

                        taskContext.OutputCount =
                            taskContext.OutputCount.AddToDictionary(resourceType, parquetStream.BatchSize);
                        taskContext.SkippedCount =
                            taskContext.SkippedCount.AddToDictionary(schemaType, skippedCount);
                        taskContext.ProcessedCount =
                            taskContext.ProcessedCount.AddToDictionary(schemaType, parquetStream.BatchSize);
                    }

                    taskContext.SearchCount =
                        taskContext.SearchCount.AddToDictionary(resourceType, resources.Count);
                    taskContext.OutputDataSize =
                        taskContext.OutputDataSize.AddToDictionary(resourceType, outputDataSize);
                }

                // update task context based on cache
                taskContext.SearchProgress = cacheResult.SearchProgress.Copy();

                // update task context to job
                await progressUpdater.Produce(taskContext, cancellationToken);

                // clear cache
                cacheResult.ClearCache();
                _logger.LogInformation($"Commit cache resources successfully for task {taskContext.TaskIndex}.");
            }
        }
    }
}
