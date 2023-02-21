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
    public class FhirToDataLakeProcessingJobSplitter
    {
        private readonly IApiDataClient _dataClient;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<FhirToDataLakeProcessingJobSplitter> _logger;
        private readonly FhirToDataLakeSplitSubJobCandidates _jobCandidatePool = new ();
        private readonly IFilterManager _filterManager;
        private const int TimeoutSecondsGetResourceCount = 90;
        private const string TotalCountFieldKey = "total";

        public FhirToDataLakeProcessingJobSplitter(
            IApiDataClient dataClient,
            IFilterManager filterManager,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirToDataLakeProcessingJobSplitter> logger)
        {
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _filterManager = EnsureArg.IsNotNull(filterManager, nameof(filterManager));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public int LowBoundOfProcessingJobResourceCount { get; set; } = JobConfigurationConstants.LowBoundOfProcessingJobResourceCount;

        public int HighBoundOfProcessingJobResourceCount { get; set; } = JobConfigurationConstants.HighBoundOfProcessingJobResourceCount;

        public async IAsyncEnumerable<FhirToDataLakeSplitProcessingJobInfo> SplitJobAsync(DateTimeOffset? dataStartTime, DateTimeOffset dataEndTime, Dictionary<string, DateTimeOffset> submittedResourceTimestamps, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            // Get all distinct resource types to be processed.
            List<TypeFilter> typeFilters = await _filterManager.GetTypeFiltersAsync(cancellationToken);
            HashSet<string> resourceTypes = typeFilters.Select(filter => filter.ResourceType).ToHashSet();

            // Split jobs by resource types.
            foreach (var resourceType in resourceTypes)
            {
                var splitJobStartTime = submittedResourceTimestamps.ContainsKey(resourceType) ? submittedResourceTimestamps[resourceType] : dataStartTime;

                // The resource type has already been processed.
                if (splitJobStartTime >= dataEndTime)
                {
                    continue;
                }

                var totalCount = await GetResourceCountAsync(resourceType, splitJobStartTime, dataEndTime, cancellationToken);

                if (totalCount == 0)
                {
                    // Return Processing job with resource count is zero.
                    _logger.LogInformation($"[Job splitter]: There is no resource for resource type {resourceType}. Will skip the resoucre type. ");
                    yield return new FhirToDataLakeSplitProcessingJobInfo
                    {
                        ResourceCount = 0,
                        SubJobInfos = new List<FhirToDataLakeSplitSubJobInfo>()
                        {
                            new ()
                            {
                                ResourceType = resourceType,
                                TimeRange = new FhirToDataLakeSplitSubJobTimeRange() { DataStartTime = splitJobStartTime, DataEndTime = dataEndTime },
                                ResourceCount = totalCount,
                            },
                        },
                    };
                }
                else if (totalCount < LowBoundOfProcessingJobResourceCount)
                {
                    // Small size job, put it into candidate pool.
                    FhirToDataLakeSplitSubJobInfo subJob = new ()
                    {
                        ResourceType = resourceType,
                        TimeRange = new FhirToDataLakeSplitSubJobTimeRange() { DataStartTime = splitJobStartTime, DataEndTime = dataEndTime },
                        ResourceCount = totalCount,
                    };

                    _jobCandidatePool.AddJobCandidates(subJob);
                    _logger.LogInformation($"[Job splitter]: One small job for {resourceType} with {totalCount} count is generated and pushed into candidate pool.");

                    // Wrap all candidates if total resource count for candidates not less than low bound. Otherwise, push sub job into candidate pool.
                    if (_jobCandidatePool.GetResourceCount() >= LowBoundOfProcessingJobResourceCount)
                    {
                        yield return _jobCandidatePool.GenerateProcessingJob();
                    }
                }
                else if (totalCount <= HighBoundOfProcessingJobResourceCount)
                {
                    // Return job with total count higher than low bound and lower than high bound.
                    _logger.LogInformation($"[Job splitter]: Split one sub job with {totalCount} resources from {splitJobStartTime} to {dataEndTime} for resource type {resourceType}.");

                    yield return new FhirToDataLakeSplitProcessingJobInfo
                    {
                        ResourceCount = totalCount,
                        SubJobInfos = new List<FhirToDataLakeSplitSubJobInfo>()
                        {
                            new FhirToDataLakeSplitSubJobInfo
                            {
                                ResourceType = resourceType,
                                TimeRange = new FhirToDataLakeSplitSubJobTimeRange() { DataStartTime = splitJobStartTime, DataEndTime = dataEndTime },
                                ResourceCount = totalCount,
                            },
                        },
                    };
                }
                else
                {
                    // Split job for one resource type if total resource count larger than high bound.
                    IAsyncEnumerable<FhirToDataLakeSplitSubJobInfo> subJobs = SplitInternalAsync(resourceType, totalCount, splitJobStartTime, dataEndTime, cancellationToken);
                    await foreach (FhirToDataLakeSplitSubJobInfo subJob in subJobs.WithCancellation(cancellationToken))
                    {
                        if (subJob.ResourceCount < LowBoundOfProcessingJobResourceCount)
                        {
                            _jobCandidatePool.AddJobCandidates(subJob);

                            // Wrap all candidates if total resource count for candidates not less than low bound. Otherwise, push sub job into candidate pool.
                            if (_jobCandidatePool.GetResourceCount() >= LowBoundOfProcessingJobResourceCount)
                            {
                                yield return _jobCandidatePool.GenerateProcessingJob();
                            }
                        }
                        else
                        {
                            yield return new FhirToDataLakeSplitProcessingJobInfo
                            {
                                ResourceCount = subJob.ResourceCount,
                                SubJobInfos = new List<FhirToDataLakeSplitSubJobInfo>() { subJob },
                            };
                        }
                    }
                }
            }

            // Wrap the rest candidates.
            if (_jobCandidatePool.GetResourceCount() > 0)
            {
                yield return _jobCandidatePool.GenerateProcessingJob();
            }
        }

        private async IAsyncEnumerable<FhirToDataLakeSplitSubJobInfo> SplitInternalAsync(string resourceType, int totalCount, DateTimeOffset? startTime, DateTimeOffset endTime, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            _logger.LogInformation($"[Job Splitter]: Start splitting job for resource type {resourceType}, total {totalCount} resources.");

            // AnchorList caches sorted anchors that record resource counts from start time.
            // Initialize anchor list to add first and last anchors in the given time range.
            SortedDictionary<DateTimeOffset, int> anchorList = await InitializeAnchorListAsync(resourceType, startTime, endTime, totalCount, cancellationToken);
            DateTimeOffset lastAnchor = anchorList.LastOrDefault().Key;

            _logger.LogInformation($"[Job Splitter]: Finish initialize anchor list for resource type {resourceType}.");

            DateTimeOffset? lastSplitTimestamp = startTime;

            int jobCount = 0;

            // Split timestamp within [startTime, endTime) in order.
            while (lastSplitTimestamp == null || lastSplitTimestamp < endTime)
            {
                DateTimeOffset? nextSplitTimestamp = await GetNextSplitTimestamp(resourceType, lastSplitTimestamp, anchorList, cancellationToken);

                int resourceCount = lastSplitTimestamp == null
                    ? anchorList[(DateTimeOffset)nextSplitTimestamp]
                    : anchorList[(DateTimeOffset)nextSplitTimestamp] - anchorList[(DateTimeOffset)lastSplitTimestamp];
                _logger.LogInformation($"[Job splitter]: Split sub job with {totalCount} resources from {lastSplitTimestamp} to {nextSplitTimestamp} for resource type {resourceType}.");

                // The last job.
                if (nextSplitTimestamp == lastAnchor)
                {
                    nextSplitTimestamp = endTime;
                }

                FhirToDataLakeSplitSubJobInfo subJob = new ()
                {
                    ResourceType = resourceType,
                    TimeRange = new FhirToDataLakeSplitSubJobTimeRange() { DataStartTime = lastSplitTimestamp, DataEndTime = (DateTimeOffset)nextSplitTimestamp },
                    ResourceCount = resourceCount,
                };

                jobCount += 1;
                lastSplitTimestamp = nextSplitTimestamp;
                yield return subJob;
            }

            _logger.LogInformation($"[Job splitter]: Finish split sub job for resource type {resourceType} with {jobCount} jobs.");
        }

        private async Task<SortedDictionary<DateTimeOffset, int>> InitializeAnchorListAsync(string resourceType, DateTimeOffset? startTime, DateTimeOffset endTime, int totalCount, CancellationToken cancellationToken)
        {
            var anchorList = new SortedDictionary<DateTimeOffset, int>();

            // Set isDescending parameter false to get first timestamp.
            DateTimeOffset? firstResourceTimestamp = await GetFirstResourceTimestamp(resourceType, startTime, endTime, cancellationToken);

            // Set isDescending parameter true to get last timestamp.
            DateTimeOffset? lastResourceTimestamp = await GetLastResourceTimestamp(resourceType, startTime, endTime, cancellationToken);

            if (firstResourceTimestamp != null)
            {
                anchorList[(DateTimeOffset)firstResourceTimestamp] = 0;
            }
            else if (startTime != null)
            {
                anchorList[(DateTimeOffset)startTime] = 0;
            }

            if (lastResourceTimestamp != null)
            {
                // the value of anchor is resource counts less than the timestamp.
                // Add 1 milliseond on the last resource timestamp.
                anchorList[((DateTimeOffset)lastResourceTimestamp).AddMilliseconds(1)] = totalCount;
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
            };

            if (isDescending)
            {
                parameters.Add(new KeyValuePair<string, string>(FhirApiConstants.SortKey, FhirApiConstants.SortByLastUpdatedDesc));
            }
            else
            {
                parameters.Add(new KeyValuePair<string, string>(FhirApiConstants.SortKey, FhirApiConstants.SortByLastUpdated));
            }

            if (start != null)
            {
                parameters.Add(new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{((DateTimeOffset)start).ToInstantString()}"));
            }

            BaseSearchOptions searchOptions = new (null, parameters);

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
                        "[Job splitter]: Failed to parse fhir search result for '{0}' with search parameters '{1}'.",
                        searchOptions.ResourceType,
                        string.Join(", ", searchOptions.QueryParameters.Select(parameter => $"{parameter.Key}: {parameter.Value}")));

                _diagnosticLogger.LogError(reason);
                _logger.LogInformation(exception, reason);
                throw new FhirDataParseException(reason, exception);
            }

            List<JObject> fhirResources = FhirBundleParser.ExtractResourcesFromBundle(fhirBundleObject).ToList();

            if (fhirResources.Any())
            {
                return fhirResources.First().GetLastUpdated();
            }

            return null;
        }

        // get next split time stamp that resource count fall in [lowBound, highBound].
        private async Task<DateTimeOffset> GetNextSplitTimestamp(string resourceType, DateTimeOffset? startTimestamp, SortedDictionary<DateTimeOffset, int> anchorList, CancellationToken cancellationToken)
        {
            int baseSize = startTimestamp == null ? 0 : anchorList[(DateTimeOffset)startTimestamp];
            DateTimeOffset? lastAnchorTimestamp = startTimestamp;
            foreach (var item in anchorList)
            {
                if (anchorList[item.Key] == int.MaxValue)
                {
                    // Get resource count failed before, re-try to get resource count.
                    var resourceCount = await GetResourceCountAsync(resourceType, lastAnchorTimestamp, item.Key, cancellationToken);
                    var lastAnchorResourceCount = lastAnchorTimestamp == null ? 0 : anchorList[(DateTimeOffset)lastAnchorTimestamp];
                    anchorList[item.Key] = resourceCount == int.MaxValue ? int.MaxValue : resourceCount + lastAnchorResourceCount;
                }

                // Find the first anchor that is more than highBound
                if (anchorList[item.Key] - baseSize < HighBoundOfProcessingJobResourceCount)
                {
                    lastAnchorTimestamp = item.Key;
                    continue;
                }
                else
                {
                    // If the last anchor is null or lower than low bound, binary search the anchor.
                    if (lastAnchorTimestamp == null || anchorList[(DateTimeOffset)lastAnchorTimestamp] - baseSize < LowBoundOfProcessingJobResourceCount)
                    {
                        // Find next split timestamp by binary search.
                        return await BinarySerachAnchor(resourceType, lastAnchorTimestamp == null ? DateTimeOffset.MinValue : (DateTimeOffset)lastAnchorTimestamp, item.Key, anchorList, baseSize, cancellationToken);
                    }
                    else
                    {
                        return (DateTimeOffset)lastAnchorTimestamp;
                    }
                }
            }

            return (DateTimeOffset)lastAnchorTimestamp;
        }

        private async Task<DateTimeOffset> BinarySerachAnchor(string resourceType, DateTimeOffset startAnchorTimestamp, DateTimeOffset endAnchorTimestamp, SortedDictionary<DateTimeOffset, int> anchorList, int baseSize, CancellationToken cancellationToken)
        {
            // Binary search to find timestamp mid that resource count between [start, mid) falls into boundaries.
            while ((endAnchorTimestamp - startAnchorTimestamp).TotalMilliseconds > 1)
            {
                DateTimeOffset midAnchorTimestamp = startAnchorTimestamp.Add((endAnchorTimestamp - startAnchorTimestamp) / 2);
                int incrementalResourceCount = await GetResourceCountAsync(resourceType, startAnchorTimestamp, midAnchorTimestamp, cancellationToken);
                int resourceCount = incrementalResourceCount == int.MaxValue ? int.MaxValue : incrementalResourceCount + anchorList[startAnchorTimestamp];
                anchorList[midAnchorTimestamp] = resourceCount;
                if (resourceCount - baseSize > HighBoundOfProcessingJobResourceCount)
                {
                    endAnchorTimestamp = midAnchorTimestamp;
                }
                else if (resourceCount - baseSize < LowBoundOfProcessingJobResourceCount)
                {
                    startAnchorTimestamp = midAnchorTimestamp;
                }
                else
                {
                    return midAnchorTimestamp;
                }
            }

            // FHIR server is not healthy if getting resource count failed within 1 millisecond search time range.
            if (anchorList[endAnchorTimestamp] == int.MaxValue)
            {
                _diagnosticLogger.LogError("[Job splitter]: Failed to split processing jobs caused by getting resource count from FHIR server failed.");
                _logger.LogInformation("[Job splitter]: Failed to split processing jobs caused by getting resource count from FHIR server failed.");
                throw new RetriableJobException("[Job splitter]: Failed to split processing jobs caused by getting resource count from FHIR server failed.");
            }

            return endAnchorTimestamp;
        }

        private async Task<int> GetResourceCountAsync(string resourceType, DateTimeOffset? startTime, DateTimeOffset endTime, CancellationToken cancellationToken)
        {
            List<KeyValuePair<string, string>> parameters = new ()
            {
                new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"lt{endTime.ToInstantString()}"),
                new KeyValuePair<string, string>(FhirApiConstants.SummaryKey, FhirApiConstants.SearchCountParameter),
                new KeyValuePair<string, string>(FhirApiConstants.TypeKey, resourceType),
            };

            if (startTime != null)
            {
                parameters.Add(new KeyValuePair<string, string>(FhirApiConstants.LastUpdatedKey, $"ge{((DateTimeOffset)startTime).ToInstantString()}"));
            }

            BaseSearchOptions searchOptions = new (null, parameters);
            string fhirBundleResult;
            using var timeoutCancellationToken = new CancellationTokenSource(TimeSpan.FromSeconds(TimeoutSecondsGetResourceCount));
            CancellationTokenSource linkedToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellationToken.Token);
            try
            {
                fhirBundleResult = await _dataClient.SearchAsync(searchOptions, linkedToken.Token);
            }
            catch (OperationCanceledException ex)
            {
                _diagnosticLogger.LogError($"[Job splitter]: Get resource count canceled. Reason: {ex.Message}. Return max int value and will retry later.");
                _logger.LogInformation(ex, "[Job splitter]: Get resource count canceled. Reason: {0}. Return max int value and will retry later.", ex.Message);
                return int.MaxValue;
            }
            catch (TimeoutRejectedException ex)
            {
                _diagnosticLogger.LogError($"[Job splitter]: Get resource count timeout. Reason: {ex.Message}. Return max int value and will retry later.");
                _logger.LogInformation(ex, "[Job splitter]: Get resource count timeout. Reason: {0}. Return max int value and will retry later.", ex.Message);
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
                        "[Job splitter]: Failed to parse fhir search result for '{0}' with search parameters '{1}'.",
                        searchOptions.ResourceType,
                        string.Join(", ", searchOptions.QueryParameters.Select(parameter => $"{parameter.Key}: {parameter.Value}")));

                _diagnosticLogger.LogError(reason);
                _logger.LogInformation(exception, reason);
                throw new FhirDataParseException(reason, exception);
            }
        }
    }
}
