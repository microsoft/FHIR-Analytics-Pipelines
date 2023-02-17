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
using DotLiquid.Util;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Models.FhirSearch;
using Microsoft.Health.AnalyticsConnector.Core.DataFilter;
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
        private readonly JobCandidatePool _jobCandidatePool = new ();
        private readonly IFilterManager _filterManager;
        private const int TimeoutSecondsGetResourceCount = 90;
        private const string TotalCountFieldKey = "total";

        public FhirToDataLakeProcessingJobSpliter(
            IApiDataClient dataClient,
            IFilterManager filterManager,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirToDataLakeProcessingJobSpliter> logger)
        {
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _filterManager = EnsureArg.IsNotNull(filterManager, nameof(filterManager));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public int LowBoundOfProcessingJobResourceCount { get; set; } = JobConfigurationConstants.LowBoundOfProcessingJobResourceCount;

        public int HighBoundOfProcessingJobResourceCount { get; set; } = JobConfigurationConstants.HighBoundOfProcessingJobResourceCount;

        public async IAsyncEnumerable<ProcessingJobInfo> SplitJobAsync(DateTimeOffset? dataStartTime, DateTimeOffset dataEndTime, Dictionary<string, DateTimeOffset> submittedResourceTimestamps, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            // Get all distinct resource types to be processed.
            List<TypeFilter> typeFilters = await _filterManager.GetTypeFiltersAsync(cancellationToken);
            var resourceTypes = typeFilters.Select(filter => filter.ResourceType).ToHashSet();

            // Split jobs by resource types.
            foreach (var resourceType in resourceTypes)
            {
                var startTime = submittedResourceTimestamps.ContainsKey(resourceType) ? submittedResourceTimestamps[resourceType] : dataStartTime;

                // The resource type has already been processed.
                if (startTime >= dataEndTime)
                {
                    continue;
                }

                var totalCount = await GetResourceCountAsync(resourceType, startTime, dataEndTime, cancellationToken);

                if (totalCount == 0)
                {
                    // Return Processing job with resource count is zero.
                    yield return new ProcessingJobInfo
                    {
                        ResourceCount = 0,
                        SubJobInfos = new List<SubJobInfo>()
                        {
                            new ()
                            {
                                ResourceType = resourceType,
                                TimeRange = new TimeRange() { DataStartTime = startTime, DataEndTime = dataEndTime },
                                ResourceCount = totalCount,
                            },
                        },
                    };
                }
                else if (totalCount < LowBoundOfProcessingJobResourceCount)
                {
                    // Small size job, put it into candidate pool.
                    SubJobInfo subJob = new ()
                    {
                        ResourceType = resourceType,
                        TimeRange = new TimeRange() { DataStartTime = startTime, DataEndTime = dataEndTime },
                        ResourceCount = totalCount,
                    };

                    _jobCandidatePool.PushJobCandidates(subJob);
                    _logger.LogInformation($"One small job for {resourceType} with {totalCount} count is generated and pushed into candidate pool.");

                    // Wrap all candidates if total resource count for candidates not less than low bound. Otherwise, push sub job into candidate pool.
                    var currentCandidatesResourceCount = _jobCandidatePool.GetResourceCount();
                    if (currentCandidatesResourceCount >= LowBoundOfProcessingJobResourceCount)
                    {
                        yield return new ProcessingJobInfo
                        {
                            ResourceCount = currentCandidatesResourceCount,
                            SubJobInfos = _jobCandidatePool.PopJobCandidates(),
                        };
                    }
                }
                else if (totalCount <= HighBoundOfProcessingJobResourceCount)
                {
                    // Return job with total count higher than low bound and lower than high bound.
                    _logger.LogInformation($"Split one sub job with {totalCount} resources from {startTime} to {dataEndTime} for resource type {resourceType}.");

                    yield return new ProcessingJobInfo
                    {
                        ResourceCount = totalCount,
                        SubJobInfos = new List<SubJobInfo>()
                        {
                            new SubJobInfo
                            {
                                ResourceType = resourceType,
                                TimeRange = new TimeRange() { DataStartTime = startTime, DataEndTime = dataEndTime },
                                ResourceCount = totalCount,
                            },
                        },
                    };
                }
                else
                {
                    // Split job for one resource type if total resource count larger than high bound.
                    IAsyncEnumerable<SubJobInfo> subJobs = SplitInternalAsync(resourceType, totalCount, startTime, dataEndTime, cancellationToken);
                    await foreach (SubJobInfo subJob in subJobs.WithCancellation(cancellationToken))
                    {
                        if (subJob.ResourceCount < LowBoundOfProcessingJobResourceCount)
                        {
                            _jobCandidatePool.PushJobCandidates(subJob);

                            // Wrap all candidates if total resource count for candidates not less than low bound. Otherwise, push sub job into candidate pool.
                            var currentCandidatesResourceCount = _jobCandidatePool.GetResourceCount();
                            if (currentCandidatesResourceCount >= LowBoundOfProcessingJobResourceCount)
                            {
                                yield return new ProcessingJobInfo
                                {
                                    ResourceCount = currentCandidatesResourceCount,
                                    SubJobInfos = _jobCandidatePool.PopJobCandidates(),
                                };
                            }
                        }
                        else
                        {
                            yield return new ProcessingJobInfo
                            {
                                ResourceCount = subJob.ResourceCount,
                                SubJobInfos = new List<SubJobInfo>() { subJob },
                            };
                        }
                    }
                }
            }

            // Wrap the rest candidates.
            if (_jobCandidatePool.GetResourceCount() > 0)
            {
                yield return new ProcessingJobInfo
                {
                    ResourceCount = _jobCandidatePool.GetResourceCount(),
                    SubJobInfos = _jobCandidatePool.PopJobCandidates(),
                };
            }
        }

        private async IAsyncEnumerable<SubJobInfo> SplitInternalAsync(string resourceType, int totalCount, DateTimeOffset? startTime, DateTimeOffset endTime, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            // Timers for logger.
            var startTimer = DateTimeOffset.UtcNow;

            _logger.LogInformation($"Start splitting {resourceType}, total {totalCount} resources.");

            // AnchorList caches sorted anchors that record resource counts from start time.
            // Initialize anchor list to add first and last anchors in the given time range.
            SortedDictionary<DateTimeOffset, int> anchorList = await InitializeAnchorListAsync(resourceType, startTime, endTime, totalCount, cancellationToken);
            var lastAnchor = anchorList.LastOrDefault().Key;

            _logger.LogInformation($"Splitting {resourceType} job. Use {(DateTimeOffset.UtcNow - startTimer).TotalMilliseconds} milliseconds to initilize anchor list.");

            DateTimeOffset? lastJobTimestamp = startTime;

            var jobCount = 0;

            // Sperate timestamp within [startTime, endTime) in order.
            while (lastJobTimestamp == null || lastJobTimestamp < endTime)
            {
                DateTimeOffset splitTimer = DateTimeOffset.UtcNow;

                var nextJobTimestamp = await GetNextSplitTimestamp(resourceType, lastJobTimestamp, anchorList, cancellationToken);

                var resourceCount = lastJobTimestamp == null
                    ? anchorList[(DateTimeOffset)nextJobTimestamp]
                    : anchorList[(DateTimeOffset)nextJobTimestamp] - anchorList[(DateTimeOffset)lastJobTimestamp];
                _logger.LogInformation($"Splitting {resourceType} job. Generated new sub job using {(DateTimeOffset.UtcNow - splitTimer).TotalMilliseconds} milliseconds with {resourceCount} resource count.");

                // The last job.
                if (nextJobTimestamp == lastAnchor)
                {
                    nextJobTimestamp = endTime;
                }

                var subJob = new SubJobInfo
                {
                    ResourceType = resourceType,
                    TimeRange = new TimeRange() { DataStartTime = lastJobTimestamp, DataEndTime = (DateTimeOffset)nextJobTimestamp },
                    ResourceCount = resourceCount,
                };

                jobCount += 1;
                lastJobTimestamp = nextJobTimestamp;
                yield return subJob;
            }

            _logger.LogInformation($"Splitting {resourceType} jobs, finish split {jobCount} jobs. use : {(DateTimeOffset.Now - startTimer).TotalMilliseconds} milliseconds.");
        }

        private async Task<SortedDictionary<DateTimeOffset, int>> InitializeAnchorListAsync(string resourceType, DateTimeOffset? startTime, DateTimeOffset endTime, int totalCount, CancellationToken cancellationToken)
        {
            var anchorList = new SortedDictionary<DateTimeOffset, int>();
            if (startTime != null)
            {
                anchorList[(DateTimeOffset)startTime] = 0;
            }

            // Set isDescending parameter false to get first timestmp.
            var firstTimestamp = await GetFirstResourceTimestamp(resourceType, startTime, endTime, cancellationToken);

            // Set isDescending parameter true to get last timestamp.
            var lastTimestamp = await GetLastResourceTimestamp(resourceType, startTime, endTime, cancellationToken);

            if (firstTimestamp != null)
            {
                anchorList[(DateTimeOffset)firstTimestamp] = 0;
            }

            if (lastTimestamp != null)
            {
                // the value of anchor is resource counts less than the timestamp.
                // Add 1 milliseond on the last resource timestamp.
                anchorList[((DateTimeOffset)lastTimestamp).AddMilliseconds(1)] = totalCount;
            }
            else
            {
                anchorList[endTime] = totalCount;
            }

            return anchorList;
        }

        // get the lastUpdated timestamp of first resource.
        private async Task<DateTimeOffset?> GetFirstResourceTimestamp(string resourceType, DateTimeOffset? startTime, DateTimeOffset endTime, CancellationToken cancellationToken)
        {
            return await GetNextTimestamp(resourceType, startTime, endTime, false, cancellationToken);
        }

        // get the lastUpdated timestamp of last resource.
        private async Task<DateTimeOffset?> GetLastResourceTimestamp(string resourceType, DateTimeOffset? startTime, DateTimeOffset endTime, CancellationToken cancellationToken)
        {
            return await GetNextTimestamp(resourceType, startTime, endTime, true, cancellationToken);
        }

        // get the lastUpdated timestamp of next resource.
        private async Task<DateTimeOffset?> GetNextTimestamp(string resourceType, DateTimeOffset? start, DateTimeOffset end, bool isDescending, CancellationToken cancellationToken)
        {
            List<KeyValuePair<string, string>> parameters = new ()
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{end.ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Single.ToString("d")),
                new KeyValuePair<string, string>(FhirApiConstants.TypeKey, resourceType),
                isDescending
                ? new KeyValuePair<string, string>(FhirApiConstants.SortKey, FhirApiConstants.LastUpdatedParameterDesc)
                : new KeyValuePair<string, string>(FhirApiConstants.SortKey, FhirApiConstants.LastUpdatedParameter),
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

        // get next split time stamp that resource count fall in [lowBound, highBound].
        private async Task<DateTimeOffset?> GetNextSplitTimestamp(string resourceType, DateTimeOffset? start, SortedDictionary<DateTimeOffset, int> anchorList, CancellationToken cancellationToken)
        {
            int baseSize = start == null ? 0 : anchorList[(DateTimeOffset)start];
            var last = start;
            foreach (var item in anchorList)
            {
                var anchorValue = item.Value;
                if (anchorList[item.Key] == int.MaxValue)
                {
                    // Get resource count failed before, re-try to get resource count.
                    var resourceCount = await GetResourceCountAsync(resourceType, last, item.Key, cancellationToken);
                    var lastAnchorValue = last == null ? 0 : anchorList[(DateTimeOffset)last];
                    anchorList[item.Key] = resourceCount == int.MaxValue ? int.MaxValue : resourceCount + lastAnchorValue;
                }

                if (anchorList[item.Key] - baseSize < LowBoundOfProcessingJobResourceCount)
                {
                    last = item.Key;
                    continue;
                }
                else if (anchorList[item.Key] - baseSize <= HighBoundOfProcessingJobResourceCount)
                {
                    return item.Key;
                }

                // Find next split timestamp by binary search.
                return await BisectAnchor(resourceType, last == null ? DateTimeOffset.MinValue : (DateTimeOffset)last, item.Key, anchorList, baseSize, cancellationToken);
            }

            return last;
        }

        private async Task<DateTimeOffset> BisectAnchor(string resourceType, DateTimeOffset start, DateTimeOffset end, SortedDictionary<DateTimeOffset, int> anchorList, int baseSize, CancellationToken cancellationToken)
        {
            // Binary search to find timestamp mid that resource count between [start, mid) falls into boundaries.
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
                new KeyValuePair<string, string>(FhirApiConstants.SummaryKey, FhirApiConstants.SearchCountParameter),
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
