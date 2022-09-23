// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Fhir
{
    public class R5FhirSpecificationProviderTests
    {
        private IFhirSpecificationProvider _r5FhirSpecificationProvider;

        private readonly NullLogger<R5FhirSpecificationProvider> _nullR5FhirSpecificationProviderLogger =
            NullLogger<R5FhirSpecificationProvider>.Instance;

        public R5FhirSpecificationProviderTests()
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            var metadataOptions = new MetadataOptions();
            dataClient.Search(metadataOptions)
                .ReturnsForAnyArgs(x => TestDataProvider.GetBundleFromFile(TestDataConstants.R5MetadataFile));

            _r5FhirSpecificationProvider = new R5FhirSpecificationProvider(dataClient, _nullR5FhirSpecificationProviderLogger);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new R5FhirSpecificationProvider(null, _nullR5FhirSpecificationProviderLogger));

            var dataClient = Substitute.For<IFhirDataClient>();

            Assert.Throws<ArgumentNullException>(
                () => new R5FhirSpecificationProvider(dataClient, null));
        }

        [Fact]
        public void GivenBrokenDataClient_WhenInitialize_ExceptionShouldBeThrown()
        {
            var dataClient = Substitute.For<IFhirDataClient>();
            dataClient.SearchAsync(default, default).ThrowsForAnyArgs(new FhirSearchException("mockException"));
            Assert.Throws<FhirSpecificationProviderException>(
                () => new R5FhirSpecificationProvider(dataClient, _nullR5FhirSpecificationProviderLogger));
        }

        [Theory]
        [InlineData("{\"invalidCapabilityStatement\": 0}")]
        [InlineData("{\"resourceType\": \"CapabilityStatement\"}")]
        [InlineData("{\"resourceType\": \"CapabilityStatement\",\"rest\": []}")]
        [InlineData("{\"resourceType\": \"CapabilityStatement\",\"rest\": [{\"mode\":\"server\"}]}")]
        public void GivenInvalidMetadata_WhenInitialize_ExceptionShouldBeThrown(string metadataContent)
        {
            var dataClient = Substitute.For<IFhirDataClient>();
            dataClient.SearchAsync(default, default).ReturnsForAnyArgs(metadataContent);

            Assert.Throws<FhirSpecificationProviderException>(
                () => new R5FhirSpecificationProvider(dataClient, _nullR5FhirSpecificationProviderLogger));
        }

        [Fact]
        public void WhenGetAllResourceTypes_TheResourcesTypeShouldBeReturned()
        {
            var types = _r5FhirSpecificationProvider.GetAllResourceTypes().ToList();
            Assert.Equal(151, types.Count);
        }

        [Theory]
        [InlineData("Patient")]
        [InlineData("InventoryReport")]
        [InlineData("EvidenceReport")]
        public void GivenValidResourceType_WhenCheckIsFhirResourceType_TrueShouldBeReturned(string type)
        {
            var isValid = _r5FhirSpecificationProvider.IsValidFhirResourceType(type);
            Assert.True(isValid);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("  ")]
        [InlineData("invalidResourceType")]
        [InlineData("patient")]
        [InlineData("PATIENT")]
        [InlineData("Patient ")]
        public void GivenInvalidResourceType_WhenCheckIsFhirResourceType_FalseShouldBeReturned(string type)
        {
            var isValid = _r5FhirSpecificationProvider.IsValidFhirResourceType(type);
            Assert.False(isValid);
        }

        [Fact]
        public void GivenValidCompartmentType_WhenGetCompartmentResourceTypes_ResourceTypesShouldBeReturned()
        {
            var types = _r5FhirSpecificationProvider.GetCompartmentResourceTypes("Patient");
            Assert.Equal(68, types.Count());
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("invalidResourceType")]
        [InlineData("Account")]
        [InlineData("Devices")]
        [InlineData("RelatedPerson")]
        [InlineData("patient")]
        [InlineData("Patient ")]
        public void GivenInvalidCompartmentType_WhenGetCompartmentResourceTypes_ExceptionShouldBeThrown(string type)
        {
            Assert.Throws<FhirSpecificationProviderException>(() => _r5FhirSpecificationProvider.GetCompartmentResourceTypes(type));
        }

        [Theory]
        [InlineData("Patient", 29)]
        [InlineData("InventoryReport", 6)]
        [InlineData("EvidenceReport", 15)]
        public void GivenValidResourceType_WhenGetSearchParametersByResourceType_SearchParametersShouldBeReturned(
            string type,
            int cnt)
        {
            var parameters = _r5FhirSpecificationProvider.GetSearchParametersByResourceType(type).ToList();
            Assert.NotEmpty(parameters);
            Assert.Equal(cnt, parameters.Count);
            Assert.Contains("_id", parameters);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("invalidResourceType")]
        [InlineData("patient")]
        [InlineData("Patient ")]
        public void GivenInvalidResourceType_WhenGetSearchParametersByResourceType_ExceptionShouldBeThrown(string type)
        {
            Assert.Throws<FhirSpecificationProviderException>(() => _r5FhirSpecificationProvider.GetSearchParametersByResourceType(type));
        }
    }
}
