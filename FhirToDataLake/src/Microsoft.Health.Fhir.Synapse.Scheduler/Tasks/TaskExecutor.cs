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
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.DataSerialization.Json;
using Microsoft.Health.Fhir.Synapse.DataSerialization.Parquet;
using Microsoft.Health.Fhir.Synapse.DataSink;
using Microsoft.Health.Fhir.Synapse.DataSource;
using Microsoft.Health.Fhir.Synapse.Scheduler.Jobs;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Tasks
{
    public sealed class TaskExecutor : ITaskExecutor
    {
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataSink;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly IJsonDataProcessor _jsonDataProcessor;
        private readonly ILogger<TaskExecutor> _logger;

        private const int ReportProgressBatchCount = 20000;

        // TODO: Refine TaskExecutor here, current TaskExecutor is more like a manager class.
        public TaskExecutor(
            IFhirDataClient dataClient,
            IFhirDataWriter dataSink,
            IColumnDataProcessor parquetDataProcessor,
            IJsonDataProcessor jsonDataProcessor,
            ILogger<TaskExecutor> logger)
        {
            EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            EnsureArg.IsNotNull(dataSink, nameof(dataSink));
            EnsureArg.IsNotNull(parquetDataProcessor, nameof(parquetDataProcessor));
            EnsureArg.IsNotNull(jsonDataProcessor, nameof(jsonDataProcessor));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataClient = dataClient;
            _dataSink = dataSink;
            _parquetDataProcessor = parquetDataProcessor;
            _jsonDataProcessor = jsonDataProcessor;
            _logger = logger;
        }

        // To do:
        // Add cancelling
        public async Task<TaskResult> ExecuteAsync(
            TaskContext taskContext,
            JobProgressUpdater progressUpdater,
            CancellationToken cancellationToken = default)
        {
            int resourceCount = 0;
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
                    var normalizedJObjectBatchData = await _jsonDataProcessor.ProcessAsync(jObjectData, taskContext);
                    var parquetStream = await _parquetDataProcessor.ProcessAsync(normalizedJObjectBatchData, taskContext, cancellationToken);
                    if (parquetStream?.Value?.Length > 0)
                    {
                        var blobUrl = await _dataSink.WriteAsync(parquetStream, taskContext, data.Key.Value, cancellationToken);
                        var batchResult = new BatchDataResult(
                            taskContext.ResourceType,
                            fhirElementsData.ContinuationToken,
                            blobUrl,
                            jObjectData.Values.Count(),
                            normalizedJObjectBatchData.Values.Count());

                        taskContext.PartId += 1;
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
                }

                // Update context
                taskContext.ContinuationToken = fhirElementsData.ContinuationToken;
                taskContext.SearchCount += fhirElementsData.Values.Count();
                taskContext.ProcessedCount = taskContext.SearchCount - taskContext.SkippedCount;
                taskContext.IsCompleted = string.IsNullOrEmpty(taskContext.ContinuationToken);

                resourceCount += fhirElementsData.Values.Count();
                if (resourceCount > 0 &&
                    (resourceCount % ReportProgressBatchCount == 0 || taskContext.IsCompleted))
                {
                    await progressUpdater.Produce(taskContext, cancellationToken);
                }
            }

            _logger.LogInformation(
                "Finished processing resource '{resourceType}'.",
                taskContext.ResourceType);

            return TaskResult.CreateFromTaskContext(taskContext);
        }
    }
}
