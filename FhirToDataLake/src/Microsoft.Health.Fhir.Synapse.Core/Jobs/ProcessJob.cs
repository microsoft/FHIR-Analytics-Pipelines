// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class ProcessJob : IJob
    {
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirDataWriter _dataWriter;
        private readonly IColumnDataProcessor _parquetDataProcessor;
        private readonly ILogger<ProcessJob> _logger;

        private JobInfo _jobInfo;
        private ProcessJobInputData _inputData;
        private ProcessJobResult _result;

        private Dictionary<string, List<JObject>> _resourceCache;
        private Dictionary<string, int> _partIds = new Dictionary<string, int>();
        private int _cacheSize = 0;
        private const int CacheSizeLimit = 10000;

        public ProcessJob(
            JobInfo jobInfo,
            ProcessJobInputData inputData,
            ProcessJobResult result,
            IFhirDataClient dataClient,
            IFhirDataWriter dataWriter,
            IColumnDataProcessor parquetDataProcessor,
            ILogger<ProcessJob> logger)
        {
            _jobInfo = jobInfo;
            _inputData = inputData;
            _result = result;
            _dataClient = dataClient;
            _dataWriter = dataWriter;
            _parquetDataProcessor = parquetDataProcessor;
            _logger = logger;

            _resourceCache = new Dictionary<string, List<JObject>>();
        }

        public async Task<string> ExecuteAsync(IProgress<string> progress, CancellationToken cancellationToken)
        {
            _result.SuccessfulResourceCount = 0;
            _result.FailedResourceCount = 0;
            _result.ProcessStartTime = DateTime.UtcNow;

            bool isCompleted = false;
            string continuationToken = null;
            while (!isCompleted)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }

                var searchParameters = new FhirSearchParameters(_inputData.ResourceTypes, _inputData.DataStart, _inputData.DataEnd, continuationToken);
                var fhirBundleResult = await _dataClient.SearchAsync(searchParameters, cancellationToken);

                // Parse bundle result.
                JObject fhirBundleObject;
                try
                {
                    fhirBundleObject = JObject.Parse(fhirBundleResult);
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Failed to parse fhir search result.");
                    throw new FhirDataParseExeption($"Failed to parse fhir search result.", exception);
                }

                continuationToken = FhirBundleParser.ExtractContinuationToken(fhirBundleObject);
                isCompleted = string.IsNullOrEmpty(continuationToken);

                var fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject);

                foreach (var fhirResource in fhirResources)
                {
                    var resourceType = fhirResource.GetResourceType();
                    if (!_resourceCache.ContainsKey(resourceType))
                    {
                        _resourceCache[resourceType] = new List<JObject>();
                    }

                    _resourceCache[resourceType].Add(fhirResource);
                    _cacheSize += 1;
                }

                if (_cacheSize > CacheSizeLimit)
                {
                    foreach (var resourceType in _resourceCache.Keys.ToList())
                    {
                        if (_resourceCache[resourceType].Any())
                        {
                            var batchData = new JsonBatchData(_resourceCache[resourceType]);
                            if (!_partIds.ContainsKey(resourceType))
                            {
                                _partIds[resourceType] = 1;
                            }

                            await ExecuteInternalAsync(batchData, resourceType, _inputData.DataEnd.Date, _partIds[resourceType], continuationToken, cancellationToken);
                            _partIds[resourceType] += 1;
                        }

                        _resourceCache[resourceType] = null;
                        _resourceCache.Remove(resourceType);
                    }
                }

                // Update context
                _result.SuccessfulResourceCount += _cacheSize;
                _cacheSize = 0;
                progress.Report(JsonConvert.SerializeObject(_result));
            }

            foreach (var resourceType in _resourceCache.Keys.ToList())
            {
                if (_resourceCache[resourceType].Any())
                {
                    var batchData = new JsonBatchData(_resourceCache[resourceType]);
                    if (!_partIds.ContainsKey(resourceType))
                    {
                        _partIds[resourceType] = 1;
                    }

                    await ExecuteInternalAsync(batchData, resourceType, _inputData.DataEnd.Date, _partIds[resourceType], continuationToken, cancellationToken);
                    _partIds[resourceType] += 1;
                }

                _resourceCache[resourceType] = new List<JObject>();
            }

            progress.Report(JsonConvert.SerializeObject(_result));

            _result.ProcessCompleteTime = DateTime.UtcNow;
            return JsonConvert.SerializeObject(_result);
        }

        private async Task ExecuteInternalAsync(JsonBatchData inputData, string resourceType, DateTime dateTime, int partId, string continuationToken, CancellationToken cancellationToken)
        {
            var processParameters = new ProcessParameters(resourceType);

            using var parquetStream = await _parquetDataProcessor.ProcessAsync(inputData, processParameters, cancellationToken);
            var skippedCount = inputData.Values.Count() - parquetStream.BatchSize;

            if (parquetStream?.Value?.Length > 0)
            {
                // Upload to blob and log result
                var blobUrl = await _dataWriter.WriteAsync(parquetStream, _jobInfo.Id, partId, dateTime, cancellationToken);

                var batchResult = new BatchDataResult(
                    resourceType,
                    parquetStream.SchemaType,
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
                    "No resource of schema type {schemaType} from {resourceType} is processed. {skippedCount} resources are skipped.",
                    parquetStream.SchemaType,
                    resourceType,
                    skippedCount);
            }
        }
    }
}
