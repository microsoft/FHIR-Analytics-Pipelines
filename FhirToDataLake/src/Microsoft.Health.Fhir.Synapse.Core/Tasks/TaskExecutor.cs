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

        private const int NumberOfResourcesPerCommit = 10000;

        // TODO: Refine TaskExecutor here, current TaskExecutor is more like a manager class.
        public TaskExecutor(
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            IFhirSchemaManager<FhirParquetSchemaNode> fhirSchemaManager,
            ILogger<TaskExecutor> logger)
        {
            EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            EnsureArg.IsNotNull(fhirSchemaManager, nameof(fhirSchemaManager));

            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataClient = dataClient;
            _dataWriter = dataWriter;
            _parquetDataProcessor = parquetDataProcessor;
            _fhirSchemaManager = fhirSchemaManager;
            _logger = logger;
        }

        // To do:
        // Add cancelling
        public async Task<TaskResult> ExecuteAsync(
            TaskContext taskContext,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException();
            }

            // Initialize cache result from the search progress of task context
            var cacheResult = new CacheResult(taskContext.SearchProgress);

            switch (taskContext.FilterScope)
            {
                case FilterScope.Group:
                {
                    // the patient resource isn't included in compartment search,
                    // so we need additional requests for patient resources if the patient resource type is required
                    if (IsPatientResourcesRequired(taskContext))
                    {
                        await GetPatientResourcesAsync(taskContext, cacheResult, progressUpdater, cancellationToken);
                    }

                    if (cacheResult.SearchProgress.Stage != TaskStage.GetResources)
                    {
                        cacheResult.SearchProgress.UpdateStage(TaskStage.GetResources);
                    }

                    for (var p = cacheResult.SearchProgress.CurrentIndex; p < taskContext.PatientIds.Count(); p++)
                    {
                        var startDateTime = taskContext.PatientIds.ToList()[p].IsNewPatient
                            ? taskContext.Since
                            : taskContext.DataPeriod.Start;
                        var parameters = new List<KeyValuePair<string, string>>
                        {
                            new (FhirApiConstants.LastUpdatedKey, $"ge{startDateTime.ToInstantString()}"),
                            new (FhirApiConstants.LastUpdatedKey, $"lt{taskContext.DataPeriod.End.ToInstantString()}"),
                        };

                        var searchOption = new CompartmentSearchOptions(
                            FhirConstants.PatientResource,
                            taskContext.PatientIds.ToList()[p].PatientId,
                            null,
                            parameters);

                        if (cacheResult.SearchProgress.CurrentIndex != p)
                        {
                            cacheResult.SearchProgress.UpdateCurrentIndex(p);
                        }

                        await SearchFiltersAsync(taskContext, searchOption, cacheResult, progressUpdater, cancellationToken);
                    }

                    break;
                }

                case FilterScope.System:
                {
                    var parameters = new List<KeyValuePair<string, string>>
                    {
                        new (FhirApiConstants.LastUpdatedKey, $"ge{taskContext.DataPeriod.Start.ToInstantString()}"),
                        new (FhirApiConstants.LastUpdatedKey, $"lt{taskContext.DataPeriod.End.ToInstantString()}"),
                    };
                    var searchOption = new BaseSearchOptions(null, parameters);

                    if (cacheResult.SearchProgress.Stage != TaskStage.GetResources)
                    {
                        cacheResult.SearchProgress.UpdateStage(TaskStage.GetResources);
                    }

                    await SearchFiltersAsync(taskContext, searchOption, cacheResult, progressUpdater, cancellationToken);
                    break;
                }

                default:
                    throw new ArgumentOutOfRangeException($"The FilterScope {taskContext.FilterScope} isn't supported now.");
            }

            await TryCommitResultAsync(taskContext, true, cacheResult, progressUpdater, cancellationToken);
            taskContext.IsCompleted = true;

            _logger.LogInformation(
                "Finished processing task '{taskIndex}'.",
                taskContext.TaskIndex);

            return TaskResult.CreateFromTaskContext(taskContext);
        }

        private static bool IsPatientResourcesRequired(TaskContext taskContext)
        {
            foreach (var typeFilter in taskContext.TypeFilters)
            {
                switch (typeFilter.ResourceType)
                {
                    case FhirConstants.PatientResource:
                        return true;
                    case FhirConstants.AllResource:
                    {
                        foreach (var (param, value) in typeFilter.Parameters)
                        {
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

        /// <summary>
        /// Get patient resources of this tasks. The resources are stored in cacheResult.
        /// </summary>
        /// <param name="taskContext">task context.</param>
        /// <param name="cacheResult">cache result.</param>
        /// <param name="progressUpdater">progress updater.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>task.</returns>
        private async Task GetPatientResourcesAsync(
            TaskContext taskContext,
            CacheResult cacheResult,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken)
        {
            // TODO: what if the request url too long?
            // move task stage to get new compartment stage
            if (cacheResult.SearchProgress.Stage == TaskStage.New)
            {
                cacheResult.SearchProgress.UpdateStage(TaskStage.GetPatientResourceFull);
            }

            // get patient resources for newly patients
            if (cacheResult.SearchProgress.Stage == TaskStage.GetPatientResourceFull &&
                cacheResult.SearchProgress.IsCurrentSearchCompleted == false)
            {
                var newPatientIds = taskContext.PatientIds.Where(x => x.IsNewPatient == true).Select(x => x.PatientId).ToHashSet();
                if (newPatientIds.Any())
                {
                    var patientParameters = new List<KeyValuePair<string, string>>
                    {
                        new (FhirApiConstants.IdKey, string.Join(',', newPatientIds)),
                    };

                    if (cacheResult.SearchProgress.ContinuationToken != null)
                    {
                        patientParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, cacheResult.SearchProgress.ContinuationToken));
                    }

                    var searchOption = new BaseSearchOptions(FhirConstants.PatientResource, patientParameters);
                    await ExecuteSearchAsync(searchOption, taskContext, cacheResult, progressUpdater, cancellationToken);
                }

                cacheResult.SearchProgress.IsCurrentSearchCompleted = true;
            }

            // move task stage to get updated compartment stage
            if (cacheResult.SearchProgress.Stage == TaskStage.GetPatientResourceFull &&
                cacheResult.SearchProgress.IsCurrentSearchCompleted)
            {
                cacheResult.SearchProgress.UpdateStage(TaskStage.GetPatientResourceIncremental);
            }

            // get updated patients
            if (cacheResult.SearchProgress.Stage == TaskStage.GetPatientResourceIncremental && cacheResult.SearchProgress.IsCurrentSearchCompleted == false)
            {
                // get updated patient resources
                var oldPatientIds = taskContext.PatientIds.Where(x => x.IsNewPatient == false).Select(x => x.PatientId).ToHashSet();
                if (oldPatientIds.Any())
                {
                    var patientParameters = new List<KeyValuePair<string, string>>
                    {
                        new (FhirApiConstants.IdKey, string.Join(',', oldPatientIds)),
                        new (FhirApiConstants.LastUpdatedKey, $"ge{taskContext.DataPeriod.Start.ToInstantString()}"),
                        new (FhirApiConstants.LastUpdatedKey, $"lt{taskContext.DataPeriod.End.ToInstantString()}"),
                    };

                    if (cacheResult.SearchProgress.ContinuationToken != null)
                    {
                        patientParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, cacheResult.SearchProgress.ContinuationToken));
                    }

                    var searchOption = new BaseSearchOptions(FhirConstants.PatientResource, patientParameters);

                    await ExecuteSearchAsync(searchOption, taskContext, cacheResult, progressUpdater, cancellationToken);
                }

                cacheResult.SearchProgress.IsCurrentSearchCompleted = true;
            }

            if (cacheResult.SearchProgress.Stage == TaskStage.GetPatientResourceIncremental && cacheResult.SearchProgress.IsCurrentSearchCompleted)
            {
                cacheResult.SearchProgress.UpdateStage(TaskStage.GetResources);
            }
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
        private async Task SearchFiltersAsync(
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
                    searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(parameter.Item1, parameter.Item2));
                }

                if (cacheResult.SearchProgress.CurrentFilter != i)
                {
                    cacheResult.SearchProgress.UpdateCurrentFilter(i);
                }

                await ExecuteSearchAsync(searchOptions, taskContext, cacheResult, progressUpdater, cancellationToken);
            }
        }

        /// <summary>
        /// Get fhir resources with the searchOption specified.
        /// the continuation token is updated for taskContext after each request, marked as IsCompleted if there is no continuation token anymore.
        /// If there is any operationOutcomes, will throw an exception
        /// </summary>
        private async Task ExecuteSearchAsync(
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

                if (cacheResult.SearchProgress.ContinuationToken != null)
                {
                    bool replacedContinuationToken = false;

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

                    if (!replacedContinuationToken)
                    {
                        searchOptions.QueryParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.ContinuationKey, cacheResult.SearchProgress.ContinuationToken));
                    }
                }

                var fhirBundleResult = await _dataClient.SearchAsync(searchOptions, cancellationToken);

                // Parse bundle result.
                JObject fhirBundleObject = null;
                try
                {
                    fhirBundleObject = JObject.Parse(fhirBundleResult);
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Failed to parse fhir search result.");
                    throw new FhirDataParseExeption($"Failed to parse fhir search result", exception);
                }

                var fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject).ToList();

                var operationOutcomes = FhirBundleParser.GetOperationOutcomes(fhirResources).ToList();
                if (operationOutcomes.Any())
                {
                    _logger.LogError($"There are operationOutcomes returned from FHIR server: {string.Join(',', operationOutcomes)}");
                    throw new FhirSearchException($"There is operationOutcome returned from FHIR server: {string.Join(',', operationOutcomes)}");
                }

                AddSearchResultToCache(fhirResources, cacheResult);

                cacheResult.SearchProgress.ContinuationToken = FhirBundleParser.ExtractContinuationToken(fhirBundleObject);

                if (string.IsNullOrEmpty(cacheResult.SearchProgress.ContinuationToken))
                {
                    cacheResult.SearchProgress.IsCurrentSearchCompleted = true;
                }

                await TryCommitResultAsync(taskContext, false, cacheResult, progressUpdater, cancellationToken);
            }
        }

        private void AddSearchResultToCache(List<JObject> fhirResources, CacheResult cacheResult)
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

        private async Task TryCommitResultAsync(
            TaskContext taskContext,
            bool forceCommit,
            CacheResult cacheResult,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken)
        {
            if (cacheResult.GetResourceCount() % NumberOfResourcesPerCommit == 0 || forceCommit)
            {
                foreach (var (resourceType, resources) in cacheResult.Resources)
                {
                    var inputData = new JsonBatchData(resources);

                    var schemaTypes = _fhirSchemaManager.GetSchemaTypes(resourceType);
                    foreach (var schemaType in schemaTypes)
                    {
                        // Convert grouped data to parquet stream
                        var processParameters = new ProcessParameters(schemaType);
                        var parquetStream = await _parquetDataProcessor.ProcessAsync(inputData, processParameters, cancellationToken);
                        var skippedCount = inputData.Values.Count() - parquetStream.BatchSize;

                        if (parquetStream?.Value?.Length > 0)
                        {
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

                        taskContext.SkippedCount =
                            taskContext.SkippedCount.AddToDictionary(schemaType, skippedCount);
                        taskContext.ProcessedCount =
                            taskContext.ProcessedCount.AddToDictionary(schemaType, parquetStream.BatchSize);
                    }

                    taskContext.SearchCount =
                        taskContext.SearchCount.AddToDictionary(resourceType, resources.Count);
                }

                // update task context based on cache
                taskContext.SearchProgress = cacheResult.SearchProgress;

                // update task context to job
                await progressUpdater.Produce(taskContext, cancellationToken);

                // clear cache
                cacheResult.Resources.Clear();
            }
        }

    }
}
