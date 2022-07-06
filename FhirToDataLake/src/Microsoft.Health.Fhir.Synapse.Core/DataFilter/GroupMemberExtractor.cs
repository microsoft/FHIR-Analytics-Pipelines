// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

extern alias FhirR4;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using FhirR4::Hl7.Fhir.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Bundle = FhirR4::Hl7.Fhir.Model.Bundle;
using FhirDateTime = Hl7.Fhir.Model.FhirDateTime;
using Group = FhirR4::Hl7.Fhir.Model.Group;
using R4FhirModelInfo = FhirR4::Hl7.Fhir.Model.ModelInfo;
using ResourceType = FhirR4::Hl7.Fhir.Model.ResourceType;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public class GroupMemberExtractor : IGroupMemberExtractor
    {
        private readonly IFhirDataClient _dataClient;
        private readonly IFhirApiDataSource _dataSource;
        private readonly ILogger<GroupMemberExtractor> _logger;

        private const string ResourceTypeCapture = "resourceType";
        private const string ResourceIdCapture = "resourceId";
        private static readonly string[] SupportedSchemes = { Uri.UriSchemeHttps, Uri.UriSchemeHttp };
        private static readonly string ResourceTypesPattern = string.Join('|', R4FhirModelInfo.SupportedResources);
        private static readonly string ReferenceCaptureRegexPattern = $@"(?<{ResourceTypeCapture}>{ResourceTypesPattern})\/(?<{ResourceIdCapture}>[A-Za-z0-9\-\.]{{1,64}})(\/_history\/[A-Za-z0-9\-\.]{{1,64}})?";

        private static readonly Regex ReferenceRegex = new (
            ReferenceCaptureRegexPattern,
            RegexOptions.Singleline | RegexOptions.Compiled | RegexOptions.ExplicitCapture);

        public GroupMemberExtractor(
            IFhirDataClient dataClient,
            IFhirApiDataSource dataSource,
            ILogger<GroupMemberExtractor> logger)
        {
            EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            EnsureArg.IsNotNull(dataSource, nameof(dataSource));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataClient = dataClient;
            _dataSource = dataSource;
            _logger = logger;
        }

        public async Task<HashSet<string>> GetGroupPatientsAsync(
            string groupId,
            List<KeyValuePair<string, string>> queryParameters,
            DateTimeOffset groupMembershipTime,
            CancellationToken cancellationToken)
        {
            return await GetGroupPatientsAsyncInternal(groupId, queryParameters, groupMembershipTime, null, true, cancellationToken);
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

            var groupMembers = await GetGroupMembersAsync(groupId, queryParameters, groupMembershipTime, isRootGroup, cancellationToken);

            // hashset is used to deduplicate patients
            var patientIds = new HashSet<string>();

            foreach (var (resourceType, resourceId ) in groupMembers)
            {
                // Only Patient resources and their compartment resources are exported. All other resource types are ignored.
                // Nested Group resources are checked to see if they contain other Patients.
                switch (resourceType)
                {
                    case FhirConstants.PatientResource:
                        var isAdd = patientIds.Add(resourceId);
                        if (!isAdd)
                        {
                            _logger.LogDebug($"The patient {resourceId} is duplicated in group.");
                        }

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
            var searchOptions = new ResourceIdSearchOptions(nameof(ResourceType.Group), groupId, queryParameters);

            // If there is an exception thrown while requesting group resource, do nothing about it and the job will crash.
            var fhirBundleResult = await _dataClient.SearchAsync(searchOptions, cancellationToken);

            var parser = new FhirJsonParser();
            Bundle bundle;
            try
            {
                bundle = parser.Parse<Bundle>(fhirBundleResult);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, $"Failed to parse fhir bundle {fhirBundleResult}.");
                throw new GroupMemberExtractorException($"Failed to parse fhir bundle {fhirBundleResult}.", exception);
            }

            var members = new List<Tuple<string, string>>();

            if (bundle.Entry == null || !bundle.Entry.Any())
            {
                // throw an exception if the specified group id isn't found.
                if (isRootGroup)
                {
                    _logger.LogError($"Group {groupId} is not found.");
                    throw new GroupMemberExtractorException($"Group {groupId} is not found.");
                }

                // for the nested group, log a warning if the group doesn't exist.
                _logger.LogWarning($"Group {groupId} is not found.");
                return members;
            }

            // OperationOutcome
            var invalidEntries = bundle.Entry.Where(entry => entry.Resource.TypeName != FhirConstants.GroupResource);
            if (invalidEntries.Any())
            {
                _logger.LogError($"There are invalid group entries returned: {string.Join(',', invalidEntries.ToString())}.");
                throw new GroupMemberExtractorException($"There are invalid group entries returned: {string.Join(',', invalidEntries.ToString())}");
            }

            // this case should not happen
            if (bundle.Entry.Count > 1)
            {
                _logger.LogError($"There are {bundle.Entry.Count} groups found for group id {groupId}.");
                throw new GroupMemberExtractorException($"There are {bundle.Entry.Count} groups found for group id {groupId}.");
            }

            if (bundle.Entry[0].Resource == null)
            {
                _logger.LogError($"The resource of Group {groupId} is null.");
                throw new GroupMemberExtractorException($"The resource of Group {groupId} is null.");
            }

            var groupResource = (Group) bundle.Entry[0].Resource;

            var fhirGroupMembershipTime = new FhirDateTime(groupMembershipTime);
            foreach (var member in groupResource.Member)
            {
                // only take the member who is active and it is in this group at fhirGroupMembershipTime
                if (member.Inactive is null or false
                    && (member.Period?.EndElement == null
                        || member.Period?.EndElement > fhirGroupMembershipTime)
                    && (member.Period?.StartElement == null
                        || member.Period?.StartElement < fhirGroupMembershipTime))
                {
                    var resourceTypeIdTuple = ResolveReference(member.Entity?.Reference);
                    if (resourceTypeIdTuple != null)
                    {
                        members.Add(resourceTypeIdTuple);
                    }
                }
            }

            return members;
        }

        /// <summary>
        /// There are several reference values defined in https://www.hl7.org/fhir/references.html.
        /// We only handle the literal references https://www.hl7.org/fhir/references-definitions.html#Reference.reference,
        /// and ignore the logical references https://www.hl7.org/fhir/references-definitions.html#Reference.identifier and reference description https://www.hl7.org/fhir/references-definitions.html#Reference.display.
        /// For literal reference, relative URL and absolute URL pointing to an internal resource are handled, while absolute URL pointing to an external resource is ignored.
        /// For all the ignored or invalid cases, we will log warning for it and return null.
        /// </summary>
        private Tuple<string, string> ResolveReference(string reference)
        {
            if (string.IsNullOrWhiteSpace(reference))
            {
                _logger.LogWarning("The reference is null or white space.");
                return null;
            }

            // the default error message
            var message = $"Fail to parse group member reference {reference}.";
            var match = ReferenceRegex.Match(reference);

            if (match.Success)
            {
                var resourceTypeInString = match.Groups[ResourceTypeCapture].Value;

                if (R4FhirModelInfo.IsKnownResource(resourceTypeInString))
                {
                    var resourceId = match.Groups[ResourceIdCapture].Value;

                    var resourceTypeStartIndex = match.Groups[ResourceTypeCapture].Index;

                    if (resourceTypeStartIndex == 0)
                    {
                        // This is relative URL.
                        return new Tuple<string, string>(resourceTypeInString, resourceId);
                    }

                    try
                    {
                        var baseUri = new Uri(reference.Substring(0, resourceTypeStartIndex), UriKind.RelativeOrAbsolute);
                        if (baseUri.IsAbsoluteUri)
                        {
                            if (baseUri.AbsoluteUri == _dataSource.FhirServerUrl)
                            {
                                // This is an absolute URL pointing to an internal resource.
                                return new Tuple<string, string>(resourceTypeInString, resourceId);
                            }

                            if (SupportedSchemes.Contains(baseUri.Scheme, StringComparer.OrdinalIgnoreCase))
                            {
                                // This is an absolute URL pointing to an external resource.
                                message = $"The reference {reference} is a absolute URL pointing to an external resource.";
                            }
                        }
                    }
                    catch (UriFormatException ex)
                    {
                        // The reference is not a relative reference but is not a valid absolute reference either.
                        message = $"The reference {reference} is not a relative reference but is not a valid absolute reference either. UriFormatException: {ex}.";
                    }
                }
                else
                {
                    message = $"The resource type {resourceTypeInString} in reference {reference} isn't a known resource type.";
                }
            }

            _logger.LogWarning(message);
            return null;
        }
    }
}