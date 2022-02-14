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
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Fhir;
using Microsoft.Health.Fhir.Synapse.DataWriter;

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
                var fhirElementsData = await _dataClient.GetAsync(searchParameters, cancellationToken);

                // Partition batch data with day
                var partitionData = from data in fhirElementsData.Values
                    group data by data.GetLastUpdatedDay()
                    into newGroup
                    orderby newGroup.Key
                    select newGroup;

                foreach (var data in partitionData)
                {
                    var originalSkippedCount = taskContext.SkippedCount;
                    var jObjectData = data.ToImmutableList().ToJsonBatchData();
                    var parquetStream = await _parquetDataProcessor.ProcessAsync(jObjectData, taskContext, cancellationToken);
                    var skippedCount = taskContext.SkippedCount - originalSkippedCount;

                    if (parquetStream?.Value?.Length > 0)
                    {
                        var blobUrl = await _dataWriter.WriteAsync(parquetStream, taskContext, data.Key.Value, cancellationToken);
                        var batchResult = new BatchDataResult(
                            taskContext.ResourceType,
                            fhirElementsData.ContinuationToken,
                            blobUrl,
                            jObjectData.Values.Count(),
                            jObjectData.Values.Count() - skippedCount);

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
    }
}
