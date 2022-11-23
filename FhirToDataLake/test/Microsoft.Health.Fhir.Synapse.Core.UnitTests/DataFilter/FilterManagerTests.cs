// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders;
using Microsoft.Health.Fhir.Synapse.DataClient.Models;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataFilter
{
    public class FilterManagerTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private readonly NullLogger<FilterManager> _nullFilterManagerLogger;
        private readonly IFhirSpecificationProvider _testFhirSpecificationProvider;

        public FilterManagerTests()
        {
            var dataClient = Substitute.For<IApiDataClient>();

            var metadataOptions = new MetadataOptions();
            dataClient.Search(metadataOptions)
                .ReturnsForAnyArgs(x => TestDataProvider.GetBundleFromFile(TestDataConstants.R4MetadataFile));

            _testFhirSpecificationProvider = new R4FhirSpecificationProvider(dataClient, _diagnosticLogger, NullLogger<R4FhirSpecificationProvider>.Instance);
            _nullFilterManagerLogger = NullLogger<FilterManager>.Instance;
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            IOptions<FilterConfiguration> fhilterConfigurationOption = Options.Create(new FilterConfiguration());

            Assert.Throws<ArgumentNullException>(
                () => new FilterManager(null, _testFhirSpecificationProvider, _diagnosticLogger, _nullFilterManagerLogger));

            Assert.Throws<ArgumentNullException>(
                () => new FilterManager(new LocalFilterProvider(Options.Create(new FilterConfiguration())), null, _diagnosticLogger, _nullFilterManagerLogger));
        }

        [Theory]
        [InlineData(null, 144)]
        [InlineData("", 144)]
        [InlineData(" ", 144)]
        [InlineData("Patient,Account", 2)]
        [InlineData("Patient", 1)]
        [InlineData("Patient,Patient", 1)]
        public async Task GivenValidTypeStringAndNullTypeFilters_WhenCreateTypeFiltersForSystem_ThenTheTypeFiltersForEachResourceTypesAreReturnedAsync(string typeString, int resourceTypeCount)
        {
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = typeString,
                TypeFilters = null,
            };

            var filterManager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);

            List<TypeFilter> typeFilters = await filterManager.GetTypeFiltersAsync(default);

            Assert.NotNull(typeFilters);
            Assert.Equal(resourceTypeCount, typeFilters.Count());
            foreach (TypeFilter typeFilter in typeFilters)
            {
                Assert.Empty(typeFilter.Parameters);
            }
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        public async Task GivenNullOrWhiteSpaceTypeStringAndNullTypeFilters_WhenCreateTypeFiltersForGroup_ThenOneTypeFilterAreReturnedAsync(string typeString)
        {
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.Group,
                RequiredTypes = typeString,
                TypeFilters = null,
            };

            var filterManager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);
            List<TypeFilter> typeFilters = await filterManager.GetTypeFiltersAsync(default);

            Assert.Single(typeFilters);
            Assert.Equal("*", typeFilters[0].ResourceType);
            Assert.Empty(typeFilters[0].Parameters);
        }

        [Theory]
        [InlineData("Patient", "Patient")] // *?_type=Patient
        [InlineData("Patient,Account", "Patient,Account")] // *?_type=Patient,Account
        [InlineData("Patient,Patient", "Patient")] // *?_type=Patient
        public async Task GivenValidTypeStringAndNullTypeFilters_WhenCreateTypeFiltersForGroup_ThenOneTypeFilterAreReturnedAsync(string typeString, string expectedTypes)
        {
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.Group,
                RequiredTypes = typeString,
                TypeFilters = null,
            };

            var filterManager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);
            List<TypeFilter> typeFilters = await filterManager.GetTypeFiltersAsync(default);

            Assert.Single(typeFilters);
            Assert.Equal("*", typeFilters[0].ResourceType);
            Assert.Single(typeFilters[0].Parameters);
            Assert.Equal("_type", typeFilters[0].Parameters[0].Key);
            Assert.Equal(expectedTypes, typeFilters[0].Parameters[0].Value);
        }

        [Fact]
        public async Task GivenEmptyStringTypeFilter_WhenParseTypeFilter_ThenTheTypeFiltersAreReturnedAsync()
        {
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = "Patient",
                TypeFilters = string.Empty,
            };

            var filterManager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);
            List<TypeFilter> typeFilters = await filterManager.GetTypeFiltersAsync(default);

            Assert.Single(typeFilters);
            Assert.Equal("Patient", typeFilters[0].ResourceType);
            Assert.Empty(typeFilters[0].Parameters);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        public async Task GivenNullOrWhiteSpaceTypeFilter_WhenParseTypeFilter_ThenTheTypeFiltersAreReturnedAsync(string filterString)
        {
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = "Patient",
                TypeFilters = filterString,
            };

            var filterManager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);
            List<TypeFilter> typeFilters = await filterManager.GetTypeFiltersAsync(default);

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

        public async Task GivenInvalidTypeString_WhenParseType_ExceptionShouldBeThrownAsync(FilterScope filterScope, string typeString)
        {
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = filterScope,
                RequiredTypes = typeString,
                TypeFilters = null,
            };

            var manager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);

            await Assert.ThrowsAsync<ConfigurationErrorException>(() =>
               manager.GetTypeFiltersAsync(default));
        }

        [Fact]
        public async Task GivenInvalidFilterScope_WhenParseType_ExceptionShouldBeThrown()
        {
            const FilterScope filterScope = (FilterScope)2;

            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = filterScope,
                RequiredTypes = null,
                TypeFilters = null,
            };

            var manager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);

            await Assert.ThrowsAsync<ConfigurationErrorException>(() =>
                manager.GetTypeFiltersAsync(default));
        }

        [Fact]
        public async Task GivenMultiValidTypeFilter_WhenParseTypeFilterForGroup_ThenTheTypeFiltersAreReturnedAsync()
        {
            const string type = "Condition,MedicationRequest";
            const string typeFilter = "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z";
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.Group,
                RequiredTypes = type,
                TypeFilters = typeFilter,
            };
            var filterManager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);
            List<TypeFilter> typeFilters = await filterManager.GetTypeFiltersAsync(default);

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
        public async Task GivenMultiValidTypeFilter_WhenParseTypeFilterForSystem_ThenTheTypeFiltersAreReturnedAsync()
        {
            const string type = "Condition,MedicationRequest";
            const string typeFilter = "MedicationRequest?status=active,MedicationRequest?status=completed&date=gt2018-07-01T00:00:00Z";

            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = type,
                TypeFilters = typeFilter,
            };

            var filterManager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);
            List<TypeFilter> typeFilters = await filterManager.GetTypeFiltersAsync(default);

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
        public async Task GivenValidTypeFilter_WhenParseTypeFilter_ThenTheTypeFiltersAreReturnedAsync(string type, string typeFilter)
        {
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = type,
                TypeFilters = typeFilter,
            };

            var filterManager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);
            List<TypeFilter> typeFilters = await filterManager.GetTypeFiltersAsync(default);

            string[] expectedTypes = type.Split(',');

            Assert.NotNull(typeFilters);

            Assert.Equal(expectedTypes.Length, typeFilters.Count);
        }

        [Theory]
        [InlineData("Observation", "Patient?gender=female")]
        [InlineData("Patient", "P?gender=female")]
        public async Task GivenTypeFilterNotInType_WhenParseTypeFilter_ExceptionShouldBeThrownAsync(string type, string typeFilter)
        {
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = type,
                TypeFilters = typeFilter,
            };

            var manager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);

            await Assert.ThrowsAsync<ConfigurationErrorException>(() =>
                manager.GetTypeFiltersAsync(default));
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
        public async Task GivenInvalidTypeFilter_WhenParseTypeFilter_ExceptionShouldBeThrownAsync(string type, string typeFilter)
        {
            var filterConfiguration = new FilterConfiguration
            {
                FilterScope = FilterScope.System,
                RequiredTypes = type,
                TypeFilters = typeFilter,
            };

            var manager = new FilterManager(
                new LocalFilterProvider(Options.Create(filterConfiguration)),
                _testFhirSpecificationProvider,
                _diagnosticLogger,
                _nullFilterManagerLogger);

            await Assert.ThrowsAsync<ConfigurationErrorException>(() =>
                manager.GetTypeFiltersAsync(default));
        }
    }
}
