﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataFilter
{
    public class GroupMemberExtractorTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        private readonly GroupMemberExtractor _groupMemberExtractor;

        private readonly FhirApiDataSource _dataSource;

        private readonly R4ReferenceParser _referenceParser;

        private readonly NullLogger<GroupMemberExtractor> _nullGroupMemberExtractorLogger =
            NullLogger<GroupMemberExtractor>.Instance;

        private readonly CancellationToken _noneCancellationToken = CancellationToken.None;

        private readonly DateTimeOffset _triggerTime = new DateTimeOffset(new DateTime(2021, 1, 1));

        private const string _testDataFolder = "DataFilter/TestData";

        private const string _sampleGroupId = "sampleGroup";
        private const string _nestedGroupId = "nestedGroup";
        private const string _invalidMembersGroupId = "invalidGroupMember";
        private const string _notFoundGroupId = "notFoundGroup";
        private const string _invalidBundle = "invalidBundle";
        private const string _invalidGroup = "invalidGroup";

        private static readonly Dictionary<string, string> _groupIdToTestDataFile = new Dictionary<string, string>
        {
            { _sampleGroupId, "sampleGroupBundle.json" },
            { _nestedGroupId, "nestedGroupBundle.json" },
            { _invalidMembersGroupId, "invalidGroupMemberBundle.json" },
            { _notFoundGroupId, "emptyBundle.json" },
            { _invalidGroup, "invalidGroupBundle.json" },
            { _invalidBundle, "invalidBundle.json" },
        };

        public GroupMemberExtractorTests()
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            dataClient.SearchAsync(new ResourceIdSearchOptions("Group", _sampleGroupId, null), _noneCancellationToken)
                .ReturnsForAnyArgs(x =>
                {
                    string resourceId = ((ResourceIdSearchOptions)x[0]).ResourceId;
                    string testDataFile = _groupIdToTestDataFile.ContainsKey(resourceId)
                        ? Path.Combine(_testDataFolder, _groupIdToTestDataFile[resourceId])
                        : TestDataConstants.EmptyBundleFile;
                    string bundle =
                        TestDataProvider.GetBundleFromFile(testDataFile);
                    return bundle;
                });

            var fhirServerConfig = new FhirServerConfiguration
            {
                ServerUrl = "https://example.com",
                Authentication = AuthenticationType.None,
            };
            IOptions<FhirServerConfiguration> fhirServerOption = Options.Create(fhirServerConfig);
            _dataSource = new FhirApiDataSource(fhirServerOption);
            _referenceParser = new R4ReferenceParser(_dataSource, NullLogger<R4ReferenceParser>.Instance);
            _groupMemberExtractor = new GroupMemberExtractor(dataClient, _referenceParser, _diagnosticLogger, _nullGroupMemberExtractorLogger);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new GroupMemberExtractor(null, _referenceParser, _diagnosticLogger, _nullGroupMemberExtractorLogger));

            var dataClient = Substitute.For<IFhirDataClient>();
            Assert.Throws<ArgumentNullException>(
                () => new GroupMemberExtractor(dataClient, null, _diagnosticLogger, _nullGroupMemberExtractorLogger));
        }

        [Fact]
        public async Task GivenAGroupId_WhenPatientsAreRequested_ThenTheActivePatientsAreReturnedAsync()
        {
            HashSet<string> resultPatientIds =
                await _groupMemberExtractor.GetGroupPatientsAsync(_sampleGroupId, null, _triggerTime, _noneCancellationToken);

            Assert.Equal(5, resultPatientIds.Count);

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
            pat9: absolute URL pointing to an internal resource
            */
            List<string> expectedPatientIds = new List<string> { "pat1", "pat3", "pat4", "pat6", "pat9" };
            List<string> unExpectedPatientIds = new List<string> { "pat2", "pat5", "pat7", "pat8" };
            foreach (string expectedPatientId in expectedPatientIds)
            {
                Assert.Contains(expectedPatientId, resultPatientIds);
            }

            foreach (string unExpectedPatientId in unExpectedPatientIds)
            {
                Assert.DoesNotContain(unExpectedPatientId, resultPatientIds);
            }
        }

        [Fact]
        public async Task
            GivenAGroupIdForAGroupWithANestedGroup_WhenPatientsAreRequested_ThenTheActivePatientsAreReturnedAsync()
        {
            HashSet<string> resultPatientIds =
                await _groupMemberExtractor.GetGroupPatientsAsync(_nestedGroupId, null, _triggerTime, _noneCancellationToken);

            Assert.Equal(7, resultPatientIds.Count);

            // emptyGroup is ignored
            // the new patient groupPat1 is included
            // the duplicate patient pat1 is included and deduplicated.
            // the duplicate patient pat2 is included.
            List<string> expectedPatientIds = new List<string> { "pat1", "pat2", "pat3", "pat4", "pat6", "pat9", "groupPat1" };
            List<string> unExpectedPatientIds = new List<string> { "pat5", "pat7", "pat8" };
            foreach (string expectedPatientId in expectedPatientIds)
            {
                Assert.Contains(expectedPatientId, resultPatientIds);
            }

            foreach (string unExpectedPatientId in unExpectedPatientIds)
            {
                Assert.DoesNotContain(unExpectedPatientId, resultPatientIds);
            }
        }

        [Theory]
        [InlineData(2010, 3)] // pat1, pat3, pat9
        [InlineData(2015, 6)] // pat1, pat3, pat4, pat6, pat7, pat9
        [InlineData(2018, 5)] // pat1, pat3, pat4, pat6, pat9
        [InlineData(2023, 6)] // pat1, pat3, pat4, pat5, pat6, pat9
        public async Task
            GivenAGroupId_WhenPatientsAreRequestedFromATime_ThenTheActivePatientsFromThatTimeAreReturnedAsync(
                int year,
                int numberOfResources)
        {
            var dateTimeOffset = new DateTimeOffset(new DateTime(year, 1, 1));
            HashSet<string> resultPatientIds = await _groupMemberExtractor.GetGroupPatientsAsync(
                _sampleGroupId,
                null,
                dateTimeOffset,
                _noneCancellationToken);

            Assert.Equal(numberOfResources, resultPatientIds.Count);
        }

        [Fact]
        public async Task GivenNoFoundGroupId_WhenGetGroupPatients_ExceptionShouldBeThrown()
        {
            var exception = await Assert.ThrowsAsync<GroupMemberExtractorException>(() => _groupMemberExtractor.GetGroupPatientsAsync(_notFoundGroupId, null, _triggerTime, _noneCancellationToken));
            Assert.StartsWith($"Failed to extract group members. Reason: Group {_notFoundGroupId} is not found.", exception.Message);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        public async Task GivenNullOrWhiteSpaceGroupId_WhenGetGroupPatients_ExceptionShouldBeThrown(string groupId)
        {
            var exception = await Assert.ThrowsAsync<GroupMemberExtractorException>(() => _groupMemberExtractor.GetGroupPatientsAsync(groupId, null, _triggerTime, _noneCancellationToken));
            Assert.Equal("Failed to extract group members. Reason: The input group id is null or white space.", exception.Message);
        }

        [Fact]
        public async Task GivenInvalidBundle_WhenGetGroupPatients_ExceptionShouldBeThrown()
        {
            var exception = await Assert.ThrowsAsync<GroupMemberExtractorException>(() => _groupMemberExtractor.GetGroupPatientsAsync(_invalidBundle, null, _triggerTime, _noneCancellationToken));
            Assert.StartsWith("Failed to extract group members. Reason: Failed to parse fhir 'Group' bundle ", exception.Message);
        }

        [Fact]
        public async Task GivenInvalidGroupBundle_WhenGetGroupPatients_ExceptionShouldBeThrown()
        {
            var exception = await Assert.ThrowsAsync<GroupMemberExtractorException>(() => _groupMemberExtractor.GetGroupPatientsAsync(_invalidGroup, null, _triggerTime, _noneCancellationToken));
            Assert.StartsWith("Failed to extract group members. Reason: There are invalid group entries returned: ", exception.Message);
        }

        [Fact]
        public async Task
            GivenAGroupIdForAGroupWithInvalidMembers_WhenPatientsAreRequested_ThenTheEmptyResultAreReturnedAsync()
        {
            HashSet<string> result = await _groupMemberExtractor.GetGroupPatientsAsync(
                _invalidMembersGroupId,
                null,
                _triggerTime,
                _noneCancellationToken);

            Assert.Empty(result);
        }
    }
}
