// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataFilter
{
    public class TypeFilterParserTests
    {
        private readonly TypeFilterParser _typeFilterParser;

        private readonly NullLogger<TypeFilterParser> _nullTypeFilterLogger =
            NullLogger<TypeFilterParser>.Instance;

        private readonly NullLogger<R4FhirSpecificationProvider> _nullR4FhirSpecificationProviderLogger =
            NullLogger<R4FhirSpecificationProvider>.Instance;

        public TypeFilterParserTests()
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            var metadataOptions = new MetadataOptions();
            dataClient.Search(metadataOptions)
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
        [InlineData(null, 145)]
        [InlineData("", 145)]
        [InlineData(" ", 145)]
        [InlineData("Patient,Account", 2)]
        [InlineData("Patient", 1)]
        [InlineData("Patient,Patient", 1)]
        public void GivenValidTypeStringAndNullTypeFilters_WhenCreateTypeFiltersForSystem_ThenTheTypeFiltersForEachResourceTypesAreReturned(string typeString, int resourceTypeCount)
        {
            var typeFilters = _typeFilterParser.CreateTypeFilters(FilterScope.System, typeString, null).ToList();

            Assert.NotNull(typeFilters);
            Assert.Equal(resourceTypeCount, typeFilters.Count());
            foreach (var typeFilter in typeFilters)
            {
                Assert.Empty(typeFilter.Parameters);
            }
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        public void GivenNullOrWhiteSpaceTypeStringAndNullTypeFilters_WhenCreateTypeFiltersForGroup_ThenOneTypeFilterAreReturned(string typeString)
        {
            var typeFilters = _typeFilterParser.CreateTypeFilters(FilterScope.Group, typeString, null).ToList();

            Assert.Single(typeFilters);
            Assert.Equal("*", typeFilters[0].ResourceType);
            Assert.Empty(typeFilters[0].Parameters);
        }

        [Theory]
        [InlineData("Patient", "Patient")] // *?_type=Patient
        [InlineData("Patient,Account", "Patient,Account")] // *?_type=Patient,Account
        [InlineData("Patient,Patient", "Patient")] // *?_type=Patient
        public void GivenValidTypeStringAndNullTypeFilters_WhenCreateTypeFiltersForGroup_ThenOneTypeFilterAreReturned(string typeString, string expectedTypes)
        {
            var typeFilters = _typeFilterParser.CreateTypeFilters(FilterScope.Group, typeString, null).ToList();

            Assert.Single(typeFilters);
            Assert.Equal("*", typeFilters[0].ResourceType);
            Assert.Single(typeFilters[0].Parameters);
            Assert.Equal("_type", typeFilters[0].Parameters[0].Key);
            Assert.Equal(expectedTypes, typeFilters[0].Parameters[0].Value);
        }

        [Fact]
        public void GivenEmptyStringTypeFilter_WhenParseTypeFilter_ThenTheTypeFiltersAreReturned()
        {
            var typeFilters = _typeFilterParser.CreateTypeFilters(FilterScope.System, "Patient", string.Empty).ToList();

            Assert.Single(typeFilters);
            Assert.Equal("Patient", typeFilters[0].ResourceType);
            Assert.Empty(typeFilters[0].Parameters);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        public void GivenNullOrWhiteSpaceTypeFilter_WhenParseTypeFilter_ThenTheTypeFiltersAreReturned(string filterString)
        {
            var typeFilters = _typeFilterParser.CreateTypeFilters(FilterScope.System, "Patient", filterString).ToList();

            Assert.Single(typeFilters);
            Assert.Equal("Patient", typeFilters[0].ResourceType);
            Assert.Empty(typeFilters[0].Parameters);
        }

        [Theory]
        [InlineData(FilterScope.Group, ",")]
        [InlineData(FilterScope.Group, "InvalidResourceType")]
        [InlineData(FilterScope.Group, "Device")]

        [InlineData(FilterScope.System, ",")]
        [InlineData(FilterScope.System, "InvalidResourceType")]
        [InlineData(FilterScope.System, "patient")]
        [InlineData(FilterScope.System, "Patient,Account,")]
        [InlineData(FilterScope.System, ",Patient,Account")]
        [InlineData(FilterScope.System, "Patient,&&,Account")]
        [InlineData(FilterScope.System, "Patient,,Account")]

        public void GivenInvalidTypeString_WhenParseType_ExceptionShouldBeThrown(FilterScope filterScope, string typeString)
        {
            Assert.Throws<ConfigurationErrorException>(() =>
                _typeFilterParser.CreateTypeFilters(filterScope, typeString, null));
        }

        [Fact]
        public void GivenInvalidFilterScope_WhenParseType_ExceptionShouldBeThrown()
        {
            const FilterScope filterScope = (FilterScope)2;
            Assert.Throws<ConfigurationErrorException>(() =>
                _typeFilterParser.CreateTypeFilters(filterScope, null, null));
        }

        [Fact]
        public void GivenMultiValidTypeFilter_WhenParseTypeFilterForGroup_ThenTheTypeFiltersAreReturned()
        {
            var type = "Condition,MedicationRequest";
            var typeFilter =
                "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z";
            var typeFilters = _typeFilterParser.CreateTypeFilters(FilterScope.Group, type, typeFilter).ToList();

            Assert.NotNull(typeFilters);
            Assert.Equal(3, typeFilters.Count());

            Assert.Equal("MedicationRequest", typeFilters[0].ResourceType);
            Assert.Single(typeFilters[0].Parameters);
            Assert.Equal("status", typeFilters[0].Parameters[0].Key);
            Assert.Equal("active", typeFilters[0].Parameters[0].Value);

            Assert.Equal("MedicationRequest", typeFilters[1].ResourceType);
            Assert.Equal(2, typeFilters[1].Parameters.Count);
            Assert.Equal("status", typeFilters[1].Parameters[0].Key);
            Assert.Equal("completed", typeFilters[1].Parameters[0].Value);
            Assert.Equal("date", typeFilters[1].Parameters[1].Key);
            Assert.Equal("gt2018-07-01T00:00:00Z", typeFilters[1].Parameters[1].Value);

            Assert.Equal("*", typeFilters[2].ResourceType);
            Assert.Single(typeFilters[2].Parameters);
            Assert.Equal("_type", typeFilters[2].Parameters[0].Key);
            Assert.Equal("Condition", typeFilters[2].Parameters[0].Value);
        }

        [Fact]
        public void GivenMultiValidTypeFilter_WhenParseTypeFilterForSystem_ThenTheTypeFiltersAreReturned()
        {
            var type = "Condition,MedicationRequest";
            var typeFilter =
                "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z";
            var typeFilters = _typeFilterParser.CreateTypeFilters(FilterScope.System, type, typeFilter).ToList();

            Assert.NotNull(typeFilters);
            Assert.Equal(3, typeFilters.Count());

            Assert.Equal("MedicationRequest", typeFilters[0].ResourceType);
            Assert.Single(typeFilters[0].Parameters);
            Assert.Equal("status", typeFilters[0].Parameters[0].Key);
            Assert.Equal("active", typeFilters[0].Parameters[0].Value);

            Assert.Equal("MedicationRequest", typeFilters[1].ResourceType);
            Assert.Equal(2, typeFilters[1].Parameters.Count);
            Assert.Equal("status", typeFilters[1].Parameters[0].Key);
            Assert.Equal("completed", typeFilters[1].Parameters[0].Value);
            Assert.Equal("date", typeFilters[1].Parameters[1].Key);
            Assert.Equal("gt2018-07-01T00:00:00Z", typeFilters[1].Parameters[1].Value);

            Assert.Equal("Condition", typeFilters[2].ResourceType);
            Assert.Empty(typeFilters[2].Parameters);
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
        public void GivenValidTypeFilter_WhenParseTypeFilter_ThenTheTypeFiltersAreReturned(string type, string typeFilter)
        {
            var typeFilters = _typeFilterParser.CreateTypeFilters(FilterScope.System, type, typeFilter).ToList();

            var expectedTypes = type.Split(',');

            Assert.NotNull(typeFilters);

            Assert.Equal(expectedTypes.Length, typeFilters.Count);
        }

        [Theory]
        [InlineData("Observation", "Patient?gender=female")]
        [InlineData("Patient", "P?gender=female")]
        public void GivenTypeFilterNotInType_WhenParseTypeFilter_ExceptionShouldBeThrown(string type, string typeFilter)
        {
            Assert.Throws<ConfigurationErrorException>(() =>
                _typeFilterParser.CreateTypeFilters(FilterScope.System, type, typeFilter));
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
                _typeFilterParser.CreateTypeFilters(FilterScope.System, type, typeFilter));
        }
    }
}
