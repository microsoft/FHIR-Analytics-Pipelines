// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataFilter
{
    public class TypeFilterParserTests
    {
        private TypeFilterParser _typeFilterParser;

        private readonly NullLogger<TypeFilterParser> _nullTypeFilterLogger =
            NullLogger<TypeFilterParser>.Instance;

        private readonly NullLogger<R4FhirSpecificationProvider> _nullR4FhirSpecificationProviderLogger =
            NullLogger<R4FhirSpecificationProvider>.Instance;

        private readonly CancellationToken _noneCancellationToken = CancellationToken.None;

        public TypeFilterParserTests()
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            dataClient.GetMetaDataAsync(_noneCancellationToken)
                .ReturnsForAnyArgs(x => TestDataProvider.GetBundleFromFile(TestDataConstants.MetadataFile));

            var fhirSpecificationProvider = new R4FhirSpecificationProvider(dataClient, _nullR4FhirSpecificationProviderLogger);

            _typeFilterParser = new TypeFilterParser(fhirSpecificationProvider, _nullTypeFilterLogger);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new TypeFilterParser(null, _nullTypeFilterLogger));
        }

        [Theory]
        [InlineData(JobType.Group, null, 66)]
        [InlineData(JobType.Group, "", 66)]
        [InlineData(JobType.Group, "Patient", 1)]
        [InlineData(JobType.Group, "Patient,Account", 2)]
        [InlineData(JobType.Group, "Patient,Patient", 1)]

        [InlineData(JobType.System, null, 145)]
        [InlineData(JobType.System, "", 145)]
        [InlineData(JobType.System, "Patient,Account", 2)]
        [InlineData(JobType.System, "Patient", 1)]
        public void GivenValidTypeString_WhenParseType_ThenTheResourceTypesAreReturned(JobType jobType, string typeString, int resourceTypeCount)
        {
            var result = _typeFilterParser.CreateDataFilter(jobType, typeString, null);

            Assert.Equal(resourceTypeCount, result.ResourceTypes.Count);
            Assert.Equal(0, result.TypeFilters.Count);
        }

        [Theory]
        [InlineData(JobType.Group, ",")]
        [InlineData(JobType.Group, "InvalidResourceType")]
        [InlineData(JobType.Group, "Device")]

        [InlineData(JobType.System, ",")]
        [InlineData(JobType.System, "InvalidResourceType")]
        [InlineData(JobType.System, "patient")]
        [InlineData(JobType.System, "Patient,Account,")]
        [InlineData(JobType.System, ",Patient,Account")]
        [InlineData(JobType.System, "Patient,&&,Account")]
        [InlineData(JobType.System, "Patient,,Account")]

        public void GivenInvalidTypeString_WhenParseType_ExceptionShouldBeThrown(JobType jobType, string typeString)
        {

            Assert.Throws<ConfigurationErrorException>(() =>
                _typeFilterParser.CreateDataFilter(jobType, typeString, null));
        }

        [Fact]
        public void GivenInvalidJobType_WhenParseType_ExceptionShouldBeThrown()
        {
            JobType jobType = (JobType)2;
            Assert.Throws<ConfigurationErrorException>(() =>
                _typeFilterParser.CreateDataFilter(jobType, null, null));
        }

        [Fact]
        public void GivenMultiValidTypeFilter_WhenParseTypeFilter_ThenTheDataFilterAreReturned()
        {
            var type = "Condition,MedicationRequest";
            var typeFilter =
                "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z";
            var result = _typeFilterParser.CreateDataFilter(JobType.Group, type, typeFilter);

            Assert.Equal(type.Split(','), result.ResourceTypes);
            Assert.Equal(2, result.TypeFilters.Count);

            var paramCount = new List<int> {1, 2};
            for (var index = 0; index < result.TypeFilters.Count; index++)
            {
                var filter = result.TypeFilters[index];
                Assert.Equal("MedicationRequest", filter.ResourceType);
                Assert.Equal(paramCount[index], filter.Parameters.Count);
            }
        }

        [Theory]
        [InlineData("Patient", "Patient?gender=female")]
        [InlineData("Patient", "Patient?_lastUpdated=gt2018-07-01T00:00:00Z")]

        // quantity
        [InlineData("Observation", "Observation?value-quantity=5.4|http://unitsofmeasure.org|mg")]

        // modifier
        [InlineData("Patient,Account", "Patient?gender:not=female")]

        // composite
        [InlineData("DiagnosticReport", "DiagnosticReport?result.code-value-quantity=http://loinc.org|2823-3$gt5.4|http://unitsofmeasure.org|mmol/L")]

        // reverse chaining
        [InlineData("Patient", "Patient?_has:Observation:patient:code=1234-5")]
        [InlineData("Patient", "Patient?_has:Observation:patient:_has:AuditEvent:entity:user=MyUserId")]

        // chained
        [InlineData("DiagnosticReport", "DiagnosticReport?subject:Patient.name=peter")]
        [InlineData("Observation", "Observation?patient.identifier=http://example.com/fhir/identifier/mrn|123456")]
        public void GivenValidTypeFilter_WhenParseTypeFilter_ThenTheDataFilterAreReturned(string type, string typeFilter)
        {
            var result = _typeFilterParser.CreateDataFilter(JobType.System, type, typeFilter);

            var expectedTypes = type.Split(',');

            Assert.Equal(expectedTypes, result.ResourceTypes);
            Assert.Equal(1, result.TypeFilters.Count);
        }

        [Fact]
        public void GivenEmptyTypeFilter_WhenParseTypeFilter_ThenTheDataFilterAreReturned()
        {
            var result = _typeFilterParser.CreateDataFilter(JobType.System, "Patient", "");

            Assert.Equal("Patient", result.ResourceTypes[0]);
            Assert.Equal(0, result.TypeFilters.Count);
        }

        [Theory]
        [InlineData("Observation", "Patient?gender=female")]
        [InlineData("Patient", "P?gender=female")]
        public void GivenTypeFilterNotInType_WhenParseTypeFilter_ExceptionShouldBeThrown(string type, string typeFilter)
        {
            Assert.Throws<ConfigurationErrorException>(() =>
                _typeFilterParser.CreateDataFilter(JobType.System, type, typeFilter));
        }

        [Theory]
        [InlineData("Patient", "invalidFilterString")]

        // without parameters
        [InlineData("Patient", "Patient?")]

        // contain empty filter
        [InlineData("Patient", ",Patient?gender=female,,")]

        // invalid parameters
        [InlineData("Patient", "Patient?invalidParams")]
        [InlineData("Patient", "Patient?a=b")]

        // unsupported parameters of fhir server
        [InlineData("Patient", "Patient?_list=42")]
        [InlineData("ActivityDefinition", "ActivityDefinition?composed-of=abc")]

        // unsupported parameters of the specified resource type
        [InlineData("Patient", "Patient?code=8310-5")]
        [InlineData("Patient", "Patient?_type=Account")]

        // search result parameters
        [InlineData("Patient", "Patient?_elements=identifier")]
        [InlineData("Patient", "Patient?_summary=count")]
        [InlineData("Patient", "Patient?_sort=_lastUpdated")]
        [InlineData("MedicationRequest", "MedicationRequest?_include=MedicationRequest:patient")]
        public void GivenInvalidTypeFilter_WhenParseTypeFilter_ExceptionShouldBeThrown(string type, string typeFilter)
        {
            Assert.Throws<ConfigurationErrorException>(() =>
                _typeFilterParser.CreateDataFilter(JobType.System, type, typeFilter));
        }
    }
}
