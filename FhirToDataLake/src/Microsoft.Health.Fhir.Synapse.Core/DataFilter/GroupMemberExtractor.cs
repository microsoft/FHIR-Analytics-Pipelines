extern alias FhirR4;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using FhirR4::Hl7.Fhir.Serialization; 
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.SearchOption;
using Bundle = FhirR4::Hl7.Fhir.Model.Bundle;
using FhirDateTime = Hl7.Fhir.Model.FhirDateTime;
using Group = FhirR4::Hl7.Fhir.Model.Group;
using ResourceType = FhirR4::Hl7.Fhir.Model.ResourceType;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public class GroupMemberExtractor : IGroupMemberExtractor
    {
        private readonly IFhirDataClient _dataClient;
        private readonly ILogger<GroupMemberExtractor> _logger;

        public GroupMemberExtractor(
            IFhirDataClient dataClient,
            ILogger<GroupMemberExtractor> logger)
        {
            EnsureArg.IsNotNull(dataClient, nameof(dataClient));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _dataClient = dataClient;
            _logger = logger;
        }

        public async Task<List<PatientWrapper>> GetGroupPatients(
            string groupId,
            List<KeyValuePair<string, string>> queryParameters,
            DateTimeOffset groupMembershipTime,
            CancellationToken cancellationToken)
        {
            var patientIds = await GetGroupPatientIds(groupId, queryParameters, groupMembershipTime, null, cancellationToken);

            return patientIds.Select(patientId => new PatientWrapper(patientId)).ToList();
        }

        /// <summary>
        /// Gets a set of Patient ids that are included in a Group, either as members or as members of nested Groups. Implicit members of a group are not included.
        /// </summary>
        /// <param name="groupId">The id of the Group.</param>
        /// <param name="queryParameters">parameters to filter groups.</param>
        /// <param name="groupMembershipTime">Only returns Patients that were in the Group at this time.</param>
        /// <param name="groupsAlreadyChecked">The already checked groups</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>A set of Patient ids</returns>
        private async Task<HashSet<string>> GetGroupPatientIds(
            string groupId,
            List<KeyValuePair<string, string>> queryParameters,
            DateTimeOffset groupMembershipTime,
            HashSet<string> groupsAlreadyChecked,
            CancellationToken cancellationToken)
        {
            groupsAlreadyChecked ??= new HashSet<string>();

            groupsAlreadyChecked.Add(groupId);

            var groupMembers = await GetGroupMembers(groupId, queryParameters, groupMembershipTime, cancellationToken);

            var patientIds = new HashSet<string>();

            foreach (var (resourceId, resourceType) in groupMembers)
            {
                // Only Patient resources and their compartment resources are exported as part of a Group export. All other resource types are ignored.
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
                            patientIds.UnionWith(await GetGroupPatientIds(resourceId, queryParameters, groupMembershipTime, groupsAlreadyChecked, cancellationToken));
                        }

                        break;
                    default:
                        // do nothing for other resource types
                        break;
                }
            }

            return patientIds;
        }

        private async Task<List<Tuple<string, string>>> GetGroupMembers (
            string groupId,
            List<KeyValuePair<string, string>> queryParameters,
            DateTimeOffset groupMembershipTime,
            CancellationToken cancellationToken)
        {
            // TODO: support both STU3 and R4
            // TODO: need to distinguish group doesn't exist and the group member is empty?
            var members = new List<Tuple<string, string>>();

            var searchOptions = new ResourceIdSearchOptions(nameof(ResourceType.Group), groupId, queryParameters);

            // TODO: handle exception in dataClient and return empty?
            var fhirBundleResult = await _dataClient.SearchAsync(searchOptions, cancellationToken);

            var parser = new FhirJsonParser();
            Bundle bundle = null;
            try
            {
                bundle = parser.Parse<Bundle>(fhirBundleResult);
            }
            catch (Exception exception)
            {
                // TODO: throw exception here?
                _logger.LogError(exception, "Failed to parse fhir search result for resource Group.");
                return members;
            }

            if (!bundle.Entry.Any())
            {
                _logger.LogError($"Group {groupId} was not found");
                return members;
            }

            if (bundle.Entry.Count > 1)
            {
                _logger.LogError($"There are {bundle.Entry.Count} groups found.");
                return members;
            }

            var groupResource = (Group) bundle.Entry[0].Resource;

            var fhirGroupMembershipTime = new FhirDateTime(groupMembershipTime);

            foreach (var member in groupResource.Member)
            {
                if (
                    (member.Inactive == null
                     || member.Inactive == false)
                    && (member.Period?.EndElement == null
                        || member.Period?.EndElement > fhirGroupMembershipTime)
                    && (member.Period?.StartElement == null
                        || member.Period?.StartElement < fhirGroupMembershipTime))
                {
                    var parameterIndex = member.Entity.Reference.IndexOf("/", StringComparison.Ordinal);

                    if (parameterIndex < 0 || parameterIndex == member.Entity.Reference.Length - 1)
                    {
                        _logger.LogError($"Fail to parse group member reference {member.Entity.Reference}");
                        continue;
                    }

                    var resourceType = member.Entity.Reference.Substring(0, parameterIndex);

                    var id = member.Entity.Reference.Substring(parameterIndex + 1);

                    members.Add(Tuple.Create(id, resourceType));
                }
            }

            return members;
        }
    }
}
