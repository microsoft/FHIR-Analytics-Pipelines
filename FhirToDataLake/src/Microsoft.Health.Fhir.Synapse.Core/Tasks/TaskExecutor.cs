// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Tasks
{
    public sealed class TaskExecutor : ITaskExecutor
    {
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly ILogger<TaskExecutor> _logger;

        private TaskQueue _taskQueue;

        private const int ReportProgressBatchCount = 10;

        // TODO: Refine TaskExecutor here, current TaskExecutor is more like a manager class.
        public TaskExecutor(
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            TaskQueue taskQueue,
            ILogger<TaskExecutor> logger)
        {
            EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataClient = dataClient;
            _dataWriter = dataWriter;
            _parquetDataProcessor = parquetDataProcessor;
            _taskQueue = taskQueue;
            _logger = logger;

        }

        // To do:
        // Add cancelling
        public async Task<TaskResult> ExecuteAsync(
            TaskContext taskContext,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken = default)
        {
            Dictionary<string, List<JObject>> _groupedData = new Dictionary<string, List<JObject>>();
            Dictionary<string, int> _dataIndexMap = new Dictionary<string, int>();
            TaskContext originContext = null;
            while (true)
            {
                taskContext = await _taskQueue.Dequeue();

                if (taskContext == null)
                {
                    Console.WriteLine($"No task polled. ");
                    break;
                }

                originContext = taskContext;

                _logger.LogInformation($"{DateTime.Now} Task {taskContext.PartId} get partients {taskContext.PatientIds.Count()}");

                taskContext.ContinuationToken = null;
                foreach (var pid in taskContext.PatientIds)
                {
                    do
                    {
                        var stopWatch = Stopwatch.StartNew();
                        var searchParameters = new FhirSearchParameters(taskContext.ResourceType, taskContext.StartTime, taskContext.EndTime, taskContext.ContinuationToken);
                        var fhirBundleResult = await _dataClient.SearchCompartmentAsync(pid, searchParameters, cancellationToken);
                        _logger.LogInformation($"Search takes {stopWatch.Elapsed.TotalSeconds}.");
                        stopWatch.Restart();

                        // Parse bundle result.
                        JObject fhirBundleObject = null;
                        try
                        {
                            fhirBundleObject = JObject.Parse(fhirBundleResult);
                        }
                        catch (Exception exception)
                        {
                            _logger.LogError(exception, "Failed to parse fhir search result for resource '{0}'.", taskContext.ResourceType);
                            throw new FhirDataParseExeption($"Failed to parse fhir search result for resource {taskContext.ResourceType}", exception);
                        }

                        var fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject);
                        taskContext.ContinuationToken = FhirBundleParser.ExtractContinuationToken(fhirBundleObject);

                        _logger.LogInformation($"Parsing json takes {stopWatch.Elapsed.TotalSeconds}.");

                        await ProcessResourcesInBundle(fhirResources, taskContext, progressUpdater, _groupedData, _dataIndexMap, cancellationToken);
                    }
                    while (taskContext.ContinuationToken != null);
                }

                _logger.LogInformation($"{DateTime.Now} Task {taskContext.PartId} completed partients {taskContext.PatientIds.Count()}");
            }

            if (originContext != null)
            {
                foreach (var resourceType in _groupedData.Keys)
                {
                    if (_groupedData[resourceType].Count() > 0)
                    {
                        await progressUpdater.Produce(new Tuple<string, int>(resourceType, _groupedData[resourceType].Count()));
                        await ProcessBatchResources(_groupedData[resourceType], resourceType, originContext, _dataIndexMap, cancellationToken);
                    }
                }
            }

            _logger.LogInformation(
                "Worker finished processing.");

            return taskContext != null ? TaskResult.CreateFromTaskContext(taskContext) : null;
        }

        private async Task ProcessResourcesInBundle(
            IEnumerable<JObject> resources,
            TaskContext taskContext,
            JobProgressUpdater progressUpdater,
            Dictionary<string, List<JObject>> _groupedData,
            Dictionary<string, int> _dataIndexMap,
            CancellationToken cancellationToken = default)
        {
            foreach (var resource in resources)
            {
                var resourceType = resource["resourceType"].ToString();
                if (!_groupedData.ContainsKey(resourceType))
                {
                    _groupedData[resourceType] = new List<JObject>();
                }

                if (_groupedData[resourceType].Count() > 4000)
                {
                    var stopWatch = Stopwatch.StartNew();

                    await ProcessBatchResources(_groupedData[resourceType], resourceType, taskContext, _dataIndexMap, cancellationToken);
                    _logger.LogInformation($"Process {resourceType} {_groupedData[resourceType].Count()} takes {stopWatch.Elapsed.TotalSeconds}.");

                    await progressUpdater.Produce(new Tuple<string, int>(resourceType, _groupedData[resourceType].Count()));
                    _groupedData[resourceType] = new List<JObject>();
                }

                if (string.Equals(resourceType, "Patient") && resource.ContainsKey("managingOrganization"))
                {
                    resource["managingOrganization"] = (resource["managingOrganization"] as JArray).First() as JObject;
                }

                _groupedData[resourceType].Add(resource);
            }
        }

        private async Task ProcessBatchResources(
            List<JObject> fhirResources,
            string resourceType,
            TaskContext taskContext,
            Dictionary<string, int> _dataIndexMap,
            CancellationToken cancellationToken = default)

        {
            // Partition batch data with day
            var partitionedDayGroups = from resource in fhirResources
                                       group resource by resource.GetLastUpdatedDay()
                                       into dayGroups
                                       orderby dayGroups.Key
                                       select dayGroups;

            foreach (var dayGroup in partitionedDayGroups)
            {
                // Convert grouped data to parquet stream
                var originalSkippedCount = taskContext.SkippedCount;
                var inputData = new JsonBatchData(dayGroup.ToImmutableList());
                var parquetStream = await _parquetDataProcessor.ProcessAsync(inputData, taskContext, cancellationToken);

                if (parquetStream?.Value?.Length > 0)
                {
                    if (!_dataIndexMap.ContainsKey(resourceType))
                    {
                        _dataIndexMap[resourceType] = 0;
                    }

                    _dataIndexMap[resourceType] += 1;

                    // Upload to blob and log result
                    var blobUrl = await _dataWriter.WriteAsync(parquetStream, $"{taskContext.JobId}_{taskContext.PartId}", resourceType, _dataIndexMap[resourceType], dayGroup.Key.Value, cancellationToken);
                    Console.WriteLine($"Uploaded to {blobUrl}");
                }
                else
                {
                    _logger.LogWarning(
                        "No resource of type {resourceType} is processed. {skippedCount} resources are skipped.",
                        taskContext.ResourceType,
                        taskContext.SkippedCount);
                }
            }
        }
    }
}
