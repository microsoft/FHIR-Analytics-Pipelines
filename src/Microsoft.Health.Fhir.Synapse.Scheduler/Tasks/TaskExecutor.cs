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
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.DataSerialization.Json;
using Microsoft.Health.Fhir.Synapse.DataSerialization.Parquet;
using Microsoft.Health.Fhir.Synapse.DataSink;
using Microsoft.Health.Fhir.Synapse.DataSource;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Tasks
{
    public sealed class TaskExecutor : ITaskExecutor
    {
        private IFhirDataClient _dataClient;
        private IFhirDataWriter _dataSink;
        private IColumnDataProcessor _parquetDataProcessor;
        private IJsonDataProcessor _jsonDataNormalizer;
        private ILogger<TaskExecutor> _logger;

        private const int ReportProgressBatchCount = 10000;

        // TODO: Refine TaskExecutor here, current TaskExecutor is more like a manager class.
        public TaskExecutor(
            IFhirDataClient dataClient,
            IFhirDataWriter dataSink,
            IColumnDataProcessor parquetDataProcessor,
            IJsonDataProcessor jsonDataNormalizer,
            ILogger<TaskExecutor> logger)
        {
            EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            EnsureArg.IsNotNull(dataSink, nameof(dataSink));
            EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            EnsureArg.IsNotNull(jsonDataNormalizer, nameof(jsonDataNormalizer));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataClient = dataClient;
            _dataSink = dataSink;
            _parquetDataProcessor = parquetDataProcessor;
            _jsonDataNormalizer = jsonDataNormalizer;
            _logger = logger;
        }

        // To do:
        // Add cancelling
        public async Task<TaskResult> ExecuteAsync(
            TaskContext taskContext,
            IProgress<TaskContext> progress,
            CancellationToken cancellationToken = default)
        {
            int processedCount = 0;
            while (!taskContext.IsCompleted)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                var fhirElementsData = await _dataClient.GetAsync(taskContext, cancellationToken);

                // Partition batch data with day
                var partitionData = from data in fhirElementsData.Values
                    group data by data.GetLastUpdatedDay()
                    into newGroup
                    orderby newGroup.Key
                    select newGroup;

                foreach (var data in partitionData)
                {
                    var jObjectData = data.ToImmutableList().ToJsonBatchData();
                    var normalizedJObjectBatchData = await _jsonDataNormalizer.ProcessAsync(jObjectData, taskContext);
                    var parquetStream = await _parquetDataProcessor.ProcessAsync(normalizedJObjectBatchData, taskContext, cancellationToken);
                    if (parquetStream?.Value?.Length > 0)
                    {
                        var batchResult = await _dataSink.WriteAsync(parquetStream, taskContext, data.Key.Value, cancellationToken);

                        taskContext.PartId += 1;
                        _logger.LogInformation(
                            "{processedCount} resources are processed. {skippedCount} resources are skipped. Detail: {detail}",
                            taskContext.ProcessedCount,
                            taskContext.SkippedCount,
                            batchResult.ToString());
                    }
                    else
                    {
                        _logger.LogWarning(
                            "No resource of type {resourceType} is processed. {skippedCount} resources are skipped.",
                            taskContext.ResourceType,
                            taskContext.SkippedCount);
                    }
                }

                // Update context
                taskContext.ContinuationToken = fhirElementsData.ContinuationToken;
                taskContext.SearchCount += fhirElementsData.Values.Count();
                taskContext.ProcessedCount = taskContext.SearchCount - taskContext.SkippedCount;
                taskContext.IsCompleted = string.IsNullOrEmpty(taskContext.ContinuationToken);

                processedCount += fhirElementsData.Values.Count();
                if (processedCount % ReportProgressBatchCount == 0)
                {
                    progress.Report(taskContext);
                }
            }

            progress.Report(taskContext);
            _logger.LogInformation(
                "Finished processing resourceType {resourceType}.",
                taskContext.ResourceType);

            return new TaskResult
            {
                Result = JsonConvert.SerializeObject(taskContext),
            };
        }
    }
}
