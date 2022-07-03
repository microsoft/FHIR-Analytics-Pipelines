// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataFilter
{
    public class GroupMemberExtractorTests
    {
        private readonly GroupMemberExtractor _groupMemberExtractor;

        private readonly NullLogger<GroupMemberExtractor> _nullGroupMemberExtractorLogger =
            NullLogger<GroupMemberExtractor>.Instance;

        private readonly CancellationToken _noneCancellationToken = CancellationToken.None;

        private const string _testDataFolder = "DataFilter/TestData";

        private readonly DateTimeOffset _triggerTime = new DateTimeOffset(new DateTime(2021, 1, 1));

        private const string _sampleGroupId = "sampleGroup";
        private const string _nestedGroupId = "nestedGroup";
        private const string _invalidMembersGroupId = "invalidGroupMember";

        private static readonly Dictionary<string, string> _groupIdToTestDataFile = new Dictionary<string, string>
        {
            {_sampleGroupId, "sampleGroupBundle.json"},
            {_nestedGroupId, "nestedGroupBundle.json"},
            {_invalidMembersGroupId, "invalidGroupMemberBundle.json"},
        };

        public GroupMemberExtractorTests()
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            dataClient.SearchAsync(new ResourceIdSearchOptions("Group", _sampleGroupId, null), _noneCancellationToken)
                .ReturnsForAnyArgs(x =>
                {
                    var resourceId = ((ResourceIdSearchOptions) x[0]).ResourceId;
                    var testDataFile = _groupIdToTestDataFile.ContainsKey(resourceId)
                        ? Path.Combine(_testDataFolder, _groupIdToTestDataFile[resourceId])
                        : TestDataConstants.EmptyBundleFile;
                    string bundle =
                        TestDataProvider.GetBundleFromFile(testDataFile);
                    return bundle;
                });

            _groupMemberExtractor = new GroupMemberExtractor(dataClient, _nullGroupMemberExtractorLogger);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new GroupMemberExtractor(null, _nullGroupMemberExtractorLogger));
        }

        [Fact]
        public async Task GivenAGroupId_WhenPatientsAreRequested_ThenTheActivePatientsAreReturnedAsync()
        {
            var result =
                await _groupMemberExtractor.GetGroupPatients(_sampleGroupId, null, _triggerTime, _noneCancellationToken);

            // all the returned patients are set as new patient initially
            foreach (var patientWrapper in result)
            {
                Assert.True(patientWrapper.IsNewPatient);
            }

            Assert.Equal(4, result.Count);

            var resultPatientIds = result.Select(p => p.PatientId).ToHashSet();

            /*
            pat1: valid, without inactive and period elements
            pat2: invalid, inactive is true
            pat3: valid, inactive is false
            pat4: valid, triggered time later than period start
            pat5: invalid, triggered time early than period start
            pat6: valid, triggered time fails in period time window
            pat7: invalid, triggered time later than period end time
            pat8: invalid, triggered time later than period start while inactive is true
            pat3: valid, duplicated patient
            pat4: invalid, duplicated patient while inactive
            */
            var expectedPatientIds = new List<string> {"pat1", "pat3", "pat4", "pat6"};
            var unExpectedPatientIds = new List<string> {"pat2", "pat5", "pat7", "pat8"};
            foreach (var expectedPatientId in expectedPatientIds)
            {
                Assert.Contains(expectedPatientId, resultPatientIds);
            }

            foreach (var unExpectedPatientId in unExpectedPatientIds)
            {
                Assert.DoesNotContain(unExpectedPatientId, resultPatientIds);
            }

        }

        [Fact]
        public async Task
            GivenAGroupIdForAGroupWithANestedGroup_WhenPatientsAreRequested_ThenTheActivePatientsAreReturnedAsync()
        {
            var result =
                await _groupMemberExtractor.GetGroupPatients(_nestedGroupId, null, _triggerTime, _noneCancellationToken);

            // all the returned patients are set as new patient initially
            foreach (var patientWrapper in result)
            {
                Assert.True(patientWrapper.IsNewPatient);
            }

            Assert.Equal(6, result.Count);

            var resultPatientIds = result.Select(p => p.PatientId).ToHashSet();

            // emptyGroup is ignored
            // the new patient groupPat1 is included
            // the duplicate patient pat1 is included and deduplicated.
            // the duplicate patient pat2 is included.
            /*
             * "member": [
                  {
                    "entity": {
                      "reference": "Patient/groupPat1"
                    }
                  },
                  {
                    "entity": {
                      "reference": "Group/sampleGroup"
                    }
                  },
                  {
                    "entity": {
                      "reference": "Group/emptyGroup"
                    }
                  },
                  {
                    "entity": {
                      "reference": "Patient/pat1"
                    }
                  },
                  {
                    "entity": {
                      "reference": "Patient/pat2"
                    }
                  }
                ]
             */
            var expectedPatientIds = new List<string> {"pat1", "pat2", "pat3", "pat4", "pat6", "groupPat1"};
            var unExpectedPatientIds = new List<string> {"pat5", "pat7", "pat8"};
            foreach (var expectedPatientId in expectedPatientIds)
            {
                Assert.Contains(expectedPatientId, resultPatientIds);
            }

            foreach (var unExpectedPatientId in unExpectedPatientIds)
            {
                Assert.DoesNotContain(unExpectedPatientId, resultPatientIds);
            }
        }

        [Theory]
        [InlineData(2010, 2)] // pat1, pat3
        [InlineData(2015, 5)] // pat1, pat3, pat4, pat6, pat7
        [InlineData(2018, 4)] // pat1, pat3, pat4, pat6
        [InlineData(2023, 5)] // pat1, pat3, pat4, pat5, pat6
        public async Task
            GivenAGroupId_WhenPatientsAreRequestedFromATime_ThenTheActivePatientsFromThatTimeAreReturnedAsync(int year,
                int numberOfResources)
        {
            var dateTimeOffset = new DateTimeOffset(new DateTime(year, 1, 1));
            var result = await _groupMemberExtractor.GetGroupPatients(
                _sampleGroupId, 
                null,
                dateTimeOffset,
                _noneCancellationToken);

            Assert.Equal(numberOfResources, result.Count);
        }

        [Fact]
        public async Task
            GivenAGroupIdForAGroupWithInvalidMembers_WhenPatientsAreRequested_ThenTheEmptyResultAreReturnedAsync()
        {
            var result = await _groupMemberExtractor.GetGroupPatients(
                _invalidMembersGroupId,
                null,
                _triggerTime,
                _noneCancellationToken);

            Assert.Empty(result);
        }
    }
}
