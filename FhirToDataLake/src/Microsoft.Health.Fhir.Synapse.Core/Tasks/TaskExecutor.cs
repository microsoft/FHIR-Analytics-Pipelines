// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
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

        private const int ReportProgressBatchCount = 10;

        // TODO: Refine TaskExecutor here, current TaskExecutor is more like a manager class.
        public TaskExecutor(
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            ILogger<TaskExecutor> logger)
        {
            EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            EnsureArg.IsNotNull(dataWriter, nameof(dataWriter));
            EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataClient = dataClient;
            _dataWriter = dataWriter;
            _parquetDataProcessor = parquetDataProcessor;
            _logger = logger;
        }

        // To do:
        // Add cancelling
        public async Task<TaskResult> ExecuteAsync(
            TaskContext taskContext,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken = default)
        {
            int pageCount = 0;
            while (!taskContext.IsCompleted)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                var searchParameters = new FhirSearchParameters(taskContext.ResourceType, taskContext.StartTime, taskContext.EndTime, taskContext.ContinuationToken);
                var fhirBundleResult = await _dataClient.SearchAsync(searchParameters, cancellationToken);

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
                var continuationToken = FhirBundleParser.ExtractContinuationToken(fhirBundleObject);

                // Partition batch data with day
                var partitionedDayGroups = from resource in fhirResources
                                           group resource by resource.GetLastUpdatedDay()
                                           into dayGroups
                                           orderby dayGroups.Key
                                           select dayGroups;

                foreach (var dayGroup in partitionedDayGroups)
                {
                    // Convert grouped data to parquet stream
                    var inputData = new JsonBatchData(dayGroup.ToImmutableList());
                    await ExecuteInternalAsync(inputData, taskContext, dayGroup.Key.Value, continuationToken, cancellationToken);
                }

                // Update context
                taskContext.ContinuationToken = continuationToken;
                taskContext.SearchCount += fhirResources.Count();
                taskContext.IsCompleted = string.IsNullOrEmpty(taskContext.ContinuationToken);

                pageCount++;
                if (pageCount % ReportProgressBatchCount == 0 || taskContext.IsCompleted)
                {
                    await progressUpdater.Produce(taskContext, cancellationToken);
                }
            }

            _logger.LogInformation(
                "Finished processing resource '{resourceType}'.",
                taskContext.ResourceType);

            return TaskResult.CreateFromTaskContext(taskContext);
        }

        private async Task ExecuteInternalAsync(
            JsonBatchData inputData,
            TaskContext taskContext,
            DateTime dateTime,
            string continuationToken,
            CancellationToken cancellationToken = default)
        {
            foreach (var schemaType in taskContext.SchemaTypes)
            {
                var processParameters = new ProcessParameters(schemaType);

                var parquetStream = await _parquetDataProcessor.ProcessAsync(inputData, processParameters, cancellationToken);
                var skippedCount = inputData.Values.Count() - parquetStream.Count;

                if (parquetStream?.Value?.Length > 0)
                {
                    // Upload to blob and log result
                    var blobUrl = await _dataWriter.WriteAsync(parquetStream, taskContext, dateTime, cancellationToken);
                    taskContext.PartId[schemaType] += 1;

                    var batchResult = new BatchDataResult(
                        taskContext.ResourceType,
                        continuationToken,
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
                        "No resource of type {resourceType} is processed. {skippedCount} resources are skipped.",
                        taskContext.ResourceType,
                        taskContext.SkippedCount);
                }

                taskContext.SkippedCount[schemaType] += skippedCount;
                taskContext.ProcessedCount[schemaType] += parquetStream.Count;
            }
        }
    }
}
