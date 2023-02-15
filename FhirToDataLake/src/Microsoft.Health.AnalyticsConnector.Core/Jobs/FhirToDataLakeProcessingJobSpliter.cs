// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.Core.Extensions;
using Microsoft.Health.AnalyticsConnector.Core.Fhir;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Fhir;
using Microsoft.Health.AnalyticsConnector.DataClient.Extensions;
using Microsoft.Health.AnalyticsConnector.DataClient.Models.FhirApiOption;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly.Timeout;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public class FhirToDataLakeProcessingJobSpliter
    {
        private readonly IApiDataClient _dataClient;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<FhirToDataLakeProcessingJobSpliter> _logger;
        private const string CountApiParameter = "count";
        private const string TotalCountFieldKey = "total";
        private const string LastUpdatedApiParameter = "_lastUpdated";
        private const string LastUpdatedApiParameterDesc = "-_lastUpdated";
        private const int TimeoutSecondsGetResourceCount = 90;

        public FhirToDataLakeProcessingJobSpliter(
            IApiDataClient dataClient,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirToDataLakeProcessingJobSpliter> logger)
        {
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public int LowBoundOfProcessingJobResourceCount { get; set; } = JobConfigurationConstants.LowBoundOfProcessingJobResourceCount;

        public int HighBoundOfProcessingJobResourceCount { get; set; } = JobConfigurationConstants.HighBoundOfProcessingJobResourceCount;

        public async IAsyncEnumerable<SubJobInfo> SplitJobAsync(string resourceType, DateTimeOffset? startTime, DateTimeOffset endTime, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var totalCount = await GetResourceCountAsync(resourceType, startTime, endTime, cancellationToken);

            if (totalCount <= HighBoundOfProcessingJobResourceCount)
            {
                // Return job with total count lower than high bound.
                _logger.LogInformation($"Split one sub job with {totalCount} resources from {startTime} to {endTime} for resource type {resourceType}.");

                yield return new SubJobInfo
                {
                    ResourceType = resourceType,
                    TimeRange = new TimeRange() { DataStartTime = startTime, DataEndTime = endTime },
                    ResourceCount = totalCount,
                };
            }
            else
            {
                // Split large size job.
                var splittingStartTime = DateTimeOffset.UtcNow;
                _logger.LogInformation($"Start splitting {resourceType} job, total {totalCount} resources.");

                SortedDictionary<DateTimeOffset, int> anchorList = await InitializeAnchorListAsync(resourceType, startTime, endTime, totalCount, cancellationToken);
                var lastTimeStamp = anchorList.LastOrDefault().Key;

                _logger.LogInformation($"Splitting {resourceType} job. Use {(DateTimeOffset.UtcNow - splittingStartTime).TotalMilliseconds} milliseconds to initilize anchor list.");
                var lastSplitTimestamp = DateTimeOffset.UtcNow;
                DateTimeOffset? nextJobEnd = null;
                var jobCount = 0;

                while (nextJobEnd == null || nextJobEnd < endTime)
                {
                    DateTimeOffset? lastEndTime = nextJobEnd ?? startTime;

                    nextJobEnd = await GetNextSplitTimestamp(resourceType, lastEndTime, anchorList, cancellationToken);

                    var resourceCount = lastEndTime == null
                        ? anchorList[(DateTimeOffset)nextJobEnd]
                        : anchorList[(DateTimeOffset)nextJobEnd] - anchorList[(DateTimeOffset)lastEndTime];
                    _logger.LogInformation($"Splitting {resourceType} job. Generated new sub job using {(DateTimeOffset.UtcNow - lastSplitTimestamp).TotalMilliseconds} milliseconds with {resourceCount} resource count.");
                    lastSplitTimestamp = DateTimeOffset.UtcNow;

                    // The last job.
                    if (nextJobEnd == lastTimeStamp)
                    {
                        nextJobEnd = endTime;
                    }

                    yield return new SubJobInfo
                    {
                        ResourceType = resourceType,
                        TimeRange = new TimeRange() { DataStartTime = lastEndTime, DataEndTime = (DateTimeOffset)nextJobEnd },
                        ResourceCount = resourceCount,
                    };

                    jobCount += 1;
                }

                _logger.LogInformation($"Splitting {resourceType} jobs, finish split {jobCount} jobs. use : {(DateTimeOffset.Now - splittingStartTime).TotalMilliseconds} milliseconds.");
            }
        }

        private async Task<SortedDictionary<DateTimeOffset, int>> InitializeAnchorListAsync(string resourceType, DateTimeOffset? startTime, DateTimeOffset endTime, int totalCount, CancellationToken cancellationToken)
        {
            var anchorList = new SortedDictionary<DateTimeOffset, int>();
            if (startTime != null)
            {
                anchorList[(DateTimeOffset)startTime] = 0;
            }

            // Set isDescending parameter false to get first timestmp.
            var nextTimestamp = await GetNextTimestamp(resourceType, startTime, endTime, false, cancellationToken);

            // Set isDescending parameter true to get last timestmp.
            var lastTimestamp = await GetNextTimestamp(resourceType, startTime, endTime, true, cancellationToken);

            if (nextTimestamp != null)
            {
                anchorList[(DateTimeOffset)nextTimestamp] = 0;
            }

            if (lastTimestamp != null)
            {
                // the value of anchor is resource counts less than the time stamp.
                // Add 1 milliseond on the last resource time stamp.
                anchorList[((DateTimeOffset)lastTimestamp).AddMilliseconds(1)] = totalCount;
            }
            else
            {
                anchorList[endTime] = totalCount;
            }

            return anchorList;
        }

        // get the lastUpdated time stamp of next resource.
        private async Task<DateTimeOffset?> GetNextTimestamp(string resourceType, DateTimeOffset? start, DateTimeOffset end, bool isDescending, CancellationToken cancellationToken)
        {
            List<KeyValuePair<string, string>> parameters = new ()
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{end.ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Single.ToString("d")),
                new KeyValuePair<string, string>(FhirApiConstants.TypeKey, resourceType),
                isDescending
                ? new KeyValuePair<string, string>(FhirApiConstants.SortKey, LastUpdatedApiParameterDesc)
                : new KeyValuePair<string, string>(FhirApiConstants.SortKey, LastUpdatedApiParameter),
            };

            if (start != null)
            {
                parameters.Add(new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{((DateTimeOffset)start).ToInstantString()}"));
            }

            var searchOptions = new BaseSearchOptions(null, parameters);

            string fhirBundleResult = await _dataClient.SearchAsync(searchOptions, cancellationToken);

            // Parse bundle result.
            JObject fhirBundleObject;
            try
            {
                fhirBundleObject = JObject.Parse(fhirBundleResult);
            }
            catch (JsonReaderException exception)
            {
                string reason = string.Format(
                        "Failed to parse fhir search result for '{0}' with search parameters '{1}'.",
                        searchOptions.ResourceType,
                        string.Join(", ", searchOptions.QueryParameters.Select(parameter => $"{parameter.Key}: {parameter.Value}")));

                _diagnosticLogger.LogError(reason);
                _logger.LogInformation(exception, reason);
                throw new FhirDataParseException(reason, exception);
            }

            List<JObject> fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject).ToList();

            if (fhirResources.Any() && fhirResources.First().GetLastUpdated() != null)
            {
                return fhirResources.First().GetLastUpdated();
            }

            return null;
        }

        private async Task<DateTimeOffset?> GetNextSplitTimestamp(string resourceType, DateTimeOffset? start, SortedDictionary<DateTimeOffset, int> anchorList, CancellationToken cancellationToken)
        {
            int baseSize = start == null ? 0 : anchorList[(DateTimeOffset)start];
            var last = start;
            foreach (var item in anchorList)
            {
                var anchorValue = item.Value;
                if (anchorValue == int.MaxValue)
                {
                    // Get resource count failed before, re-try to get resource count.
                    var resourceCount = await GetResourceCountAsync(resourceType, last, item.Key, cancellationToken);
                    var lastAnchorValue = last == null ? 0 : anchorList[(DateTimeOffset)last];
                    anchorList[item.Key] = resourceCount == int.MaxValue ? int.MaxValue : resourceCount + lastAnchorValue;
                    anchorValue = anchorList[item.Key];
                }

                if (anchorValue - baseSize < LowBoundOfProcessingJobResourceCount)
                {
                    last = item.Key;
                    continue;
                }

                if (anchorValue - baseSize <= HighBoundOfProcessingJobResourceCount && anchorValue - baseSize >= LowBoundOfProcessingJobResourceCount)
                {
                    return item.Key;
                }

                // Find next split time stamp by binary search.
                return await BisectAnchor(resourceType, last == null ? DateTimeOffset.MinValue : (DateTimeOffset)last, item.Key, anchorList, baseSize, cancellationToken);
            }

            return last;
        }

        private async Task<DateTimeOffset> BisectAnchor(string resourceType, DateTimeOffset start, DateTimeOffset end, SortedDictionary<DateTimeOffset, int> anchorList, int baseSize, CancellationToken cancellationToken)
        {
            while ((end - start).TotalMilliseconds > 1)
            {
                DateTimeOffset mid = start.Add((end - start) / 2);
                var resourceCount = await GetResourceCountAsync(resourceType, start, mid, cancellationToken);
                resourceCount = resourceCount == int.MaxValue ? int.MaxValue : resourceCount + anchorList[start];
                anchorList[mid] = resourceCount;
                if (resourceCount - baseSize > HighBoundOfProcessingJobResourceCount)
                {
                    end = mid;
                }
                else if (resourceCount - baseSize < LowBoundOfProcessingJobResourceCount)
                {
                    start = mid;
                }
                else
                {
                    return mid;
                }
            }

            // FHIR server has internal error if getting resource count failed within 1 millisecond search time range.
            if (anchorList[end] == int.MaxValue)
            {
                _diagnosticLogger.LogError("Failed to split processing jobs caused by getting resource count from FHIR server failed.");
                _logger.LogInformation("Failed to split processing jobs caused by getting resource count from FHIR server failed.");
                throw new RetriableJobException("Failed to split processing jobs caused by getting resource count from FHIR server failed.");
            }

            return end;
        }

        private async Task<int> GetResourceCountAsync(string resourceType, DateTimeOffset? start, DateTimeOffset end, CancellationToken cancellationToken)
        {
            List<KeyValuePair<string, string>> parameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{end.ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.SummaryKey, CountApiParameter),
                new KeyValuePair<string, string>(FhirApiConstants.TypeKey, resourceType),
            };

            if (start != null)
            {
                parameters.Add(new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{((DateTimeOffset)start).ToInstantString()}"));
            }

            var searchOptions = new BaseSearchOptions(null, parameters);
            string fhirBundleResult;
            using var timeoutCancellationToken = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSecondsGetResourceCount));
            CancellationTokenSource linkedToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellationToken.Token);
            try
            {
                fhirBundleResult = await _dataClient.SearchAsync(searchOptions, linkedToken.Token);
            }
            catch (OperationCanceledException ex)
            {
                _diagnosticLogger.LogError($"Get resource count canceled. Reason: {ex.Message}. Return max int value and will retry later.");
                _logger.LogInformation(ex, "Get resource count canceled. Reason: {0}. Return max int value and will retry later.", ex.Message);
                return int.MaxValue;
            }
            catch (TimeoutRejectedException ex)
            {
                _diagnosticLogger.LogError($"Get resource count timeout. Reason: {ex.Message}. Return max int value and will retry later.");
                _logger.LogInformation(ex, "Get resource count timeout. Reason: {0}. Return max int value and will retry later.", ex.Message);
                return int.MaxValue;
            }

            // Parse bundle result.
            JObject fhirBundleObject;
            try
            {
                fhirBundleObject = JObject.Parse(fhirBundleResult);
                return (int)fhirBundleObject[TotalCountFieldKey];
            }
            catch (JsonReaderException exception)
            {
                string reason = string.Format(
                        "Failed to parse fhir search result for '{0}' with search parameters '{1}'.",
                        searchOptions.ResourceType,
                        string.Join(", ", searchOptions.QueryParameters.Select(parameter => $"{parameter.Key}: {parameter.Value}")));

                _diagnosticLogger.LogError(reason);
                _logger.LogInformation(exception, reason);
                throw new FhirDataParseException(reason, exception);
            }
        }
    }
}
