﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

extern alias FhirR4;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using FhirR4::Hl7.Fhir.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Models.FhirSearch;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.Core.Fhir;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataClient.Models.FhirApiOption;
using Bundle = FhirR4::Hl7.Fhir.Model.Bundle;
using FhirDateTime = Hl7.Fhir.Model.FhirDateTime;
using Group = FhirR4::Hl7.Fhir.Model.Group;
using ResourceType = FhirR4::Hl7.Fhir.Model.ResourceType;

namespace Microsoft.Health.AnalyticsConnector.Core.DataFilter
{
    public class GroupMemberExtractor : IGroupMemberExtractor
    {
        private readonly IApiDataClient _dataClient;
        private readonly IReferenceParser _referenceParser;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<GroupMemberExtractor> _logger;

        public GroupMemberExtractor(
            IApiDataClient dataClient,
            IReferenceParser referenceParser,
            IDiagnosticLogger diagnosticLogger,
            ILogger<GroupMemberExtractor> logger)
        {
            _dataClient = EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            _referenceParser = EnsureArg.IsNotNull(referenceParser, nameof(referenceParser));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public async Task<HashSet<string>> GetGroupPatientsAsync(
            string groupId,
            List<KeyValuePair<string, string>> queryParameters,
            DateTimeOffset groupMembershipTime,
            CancellationToken cancellationToken)
        {
            return await GetGroupPatientsAsyncInternal(
                groupId,
                queryParameters,
                groupMembershipTime,
                null,
                true,
                cancellationToken);
        }

        /// <summary>
        /// Internal method to get group patients, skip process groups in "groupsAlreadyChecked" set.
        /// </summary>
        private async Task<HashSet<string>> GetGroupPatientsAsyncInternal(
            string groupId,
            List<KeyValuePair<string, string>> queryParameters,
            DateTimeOffset groupMembershipTime,
            HashSet<string> groupsAlreadyChecked,
            bool isRootGroup,
            CancellationToken cancellationToken)
        {
            groupsAlreadyChecked ??= new HashSet<string>();

            groupsAlreadyChecked.Add(groupId);

            List<Tuple<string, string>> groupMembers = await GetGroupMembersAsync(
                groupId,
                queryParameters,
                groupMembershipTime,
                isRootGroup,
                cancellationToken);

            // hashset is used to deduplicate patients
            HashSet<string> patientIds = new HashSet<string>();

            foreach ((string resourceType, string resourceId) in groupMembers)
            {
                // Only Patient resources and their compartment resources are exported. All other resource types are ignored.
                // Nested Group resources are checked to see if they contain other Patients.
                switch (resourceType)
                {
                    case FhirConstants.PatientResource:
                        patientIds.Add(resourceId);
                        break;
                    case FhirConstants.GroupResource:
                        // need to check that loops aren't happening
                        if (!groupsAlreadyChecked.Contains(resourceId))
                        {
                            // handle nested group
                            patientIds.UnionWith(await GetGroupPatientsAsyncInternal(resourceId, queryParameters, groupMembershipTime, groupsAlreadyChecked, false, cancellationToken));
                        }

                        break;
                    default:
                        // do nothing for other resource types
                        break;
                }
            }

            return patientIds;
        }

        /// <summary>
        /// Request group resource from fhir server and extract the group members.
        /// </summary>
        // TODO: this method parses result as R4 resource now. We need to define a interface and implement both R4 and Stu3 version to support both STU3 and R4 in the future.
        private async Task<List<Tuple<string, string>>> GetGroupMembersAsync(
            string groupId,
            List<KeyValuePair<string, string>> queryParameters,
            DateTimeOffset groupMembershipTime,
            bool isRootGroup,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(groupId))
            {
                _diagnosticLogger.LogError("Failed to extract group members. Reason: The input group id is null or white space.");
                _logger.LogInformation("Failed to extract group members. Reason: The input group id is null or white space.");
                throw new GroupMemberExtractorException("Failed to extract group members. Reason: The input group id is null or white space.");
            }

            var searchOptions = new ResourceIdSearchOptions(nameof(ResourceType.Group), groupId, queryParameters);

            // If there is an exception thrown while requesting group resource, do nothing about it and the job will crash.
            string fhirBundleResult = await _dataClient.SearchAsync(searchOptions, cancellationToken);

            var parser = new FhirJsonParser();
            Bundle bundle;
            try
            {
                bundle = parser.Parse<Bundle>(fhirBundleResult);
            }
            catch (Exception exception)
            {
                _diagnosticLogger.LogError($"Failed to extract group members. Reason: Failed to parse fhir 'Group' bundle {groupId}.");
                _logger.LogInformation(exception, $"Failed to extract group members. Reason: Failed to parse fhir 'Group' bundle {groupId}.");
                throw new GroupMemberExtractorException($"Failed to extract group members. Reason: Failed to parse fhir 'Group' bundle {groupId}.", exception);
            }

            List<Tuple<string, string>> members = new List<Tuple<string, string>>();

            if (bundle.Entry == null || !bundle.Entry.Any())
            {
                // throw an exception if the specified group id isn't found.
                if (isRootGroup)
                {
                    _diagnosticLogger.LogError($"Failed to extract group members. Reason: Group {groupId} is not found.");
                    _logger.LogInformation($"Failed to extract group members. Reason: Group {groupId} is not found.");
                    throw new GroupMemberExtractorException($"Failed to extract group members. Reason: Group {groupId} is not found.");
                }

                // for the nested group, log a warning if the group doesn't exist.
                _logger.LogInformation($"Group {groupId} is not found.");
                return members;
            }

            // OperationOutcome
            IEnumerable<Bundle.EntryComponent> invalidEntries = bundle.Entry.Where(entry => entry.Resource.TypeName != FhirConstants.GroupResource);
            if (invalidEntries.Any())
            {
                _diagnosticLogger.LogError($"Failed to extract group members. Reason: There are invalid group entries returned: {string.Join(',', invalidEntries.ToString())}.");
                _logger.LogInformation(
                    $"Failed to extract group members. Reason: There are invalid group entries returned: {string.Join(',', invalidEntries.ToString())}.");
                throw new GroupMemberExtractorException(
                    $"Failed to extract group members. Reason: There are invalid group entries returned: {string.Join(',', invalidEntries.ToString())}");
            }

            // this case should not happen
            if (bundle.Entry.Count > 1)
            {
                _diagnosticLogger.LogError($"Failed to extract group members. Reason: There are {bundle.Entry.Count} groups found for group id {groupId}.");
                _logger.LogInformation($"Failed to extract group members. Reason: There are {bundle.Entry.Count} groups found for group id {groupId}.");
                throw new GroupMemberExtractorException(
                    $"Failed to extract group members. Reason: There are {bundle.Entry.Count} groups found for group id {groupId}.");
            }

            if (bundle.Entry[0].Resource == null)
            {
                _diagnosticLogger.LogError($"Failed to extract group members. Reason: The resource of Group {groupId} is null.");
                _logger.LogInformation($"Failed to extract group members. Reason: The resource of Group {groupId} is null.");
                throw new GroupMemberExtractorException($"Failed to extract group members. Reason: The resource of Group {groupId} is null.");
            }

            var groupResource = (Group)bundle.Entry[0].Resource;

            var fhirGroupMembershipTime = new FhirDateTime(groupMembershipTime);
            foreach (Group.MemberComponent member in groupResource.Member)
            {
                // only take the member who is active and it is in this group at fhirGroupMembershipTime
                if (member.Inactive is null or false
                    && (member.Period?.EndElement == null
                        || member.Period?.EndElement > fhirGroupMembershipTime)
                    && (member.Period?.StartElement == null
                        || member.Period?.StartElement < fhirGroupMembershipTime))
                {
                    try
                    {
                        FhirReference fhirReference = _referenceParser.Parse(reference: member.Entity?.Reference);
                        members.Add(Tuple.Create(fhirReference.ResourceType, fhirReference.ResourceId));
                    }
                    catch (Exception ex)
                    {
                        _logger.LogInformation(ex, $"Failed to extract group members. Reason: Failed to parse reference {member.Entity?.Reference}, exception {ex}");
                    }
                }
            }

            return members;
        }
    }
}