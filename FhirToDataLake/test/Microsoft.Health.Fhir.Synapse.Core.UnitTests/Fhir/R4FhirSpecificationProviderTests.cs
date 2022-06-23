// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Fhir
{
    public class R4FhirSpecificationProviderTests
    {
        private readonly IFhirSpecificationProvider _fhirSpecificationProvider;
        private readonly NullLogger<R4FhirSpecificationProvider> _nullR4FhirSpecificationProviderLogger =
            NullLogger<R4FhirSpecificationProvider>.Instance;

        private readonly CancellationToken _noneCancellationToken = CancellationToken.None;

        public R4FhirSpecificationProviderTests()
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            dataClient.GetMetaDataAsync(_noneCancellationToken)
                .ReturnsForAnyArgs(x => TestDataProvider.GetBundleFromFile(TestDataConstants.MetadataFile));

            _fhirSpecificationProvider = new R4FhirSpecificationProvider(dataClient, _nullR4FhirSpecificationProviderLogger);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new R4FhirSpecificationProvider(null, _nullR4FhirSpecificationProviderLogger));
        }

        [Fact]
        public void GivenInvalidMetadata_WhenInitialize_ExceptionShouldBeThrown()
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            dataClient.GetMetaDataAsync(_noneCancellationToken)
                .ReturnsForAnyArgs(x => "");

            Assert.Throws<FhirSpecificationProviderException>(
                () => new R4FhirSpecificationProvider(dataClient, _nullR4FhirSpecificationProviderLogger));
        }

        [Fact]
        public void WhenGetAllResourceTypes_ThenTheResourceTypeAreReturned()
        {
            int resourceCount = _fhirSpecificationProvider.GetAllResourceTypes().Count();
            Assert.Equal(145, resourceCount);
        }

        [Theory]
        [InlineData("Patient")]
        [InlineData("Account")]
        [InlineData("Device")]

        public void GivenValidResourceType_WhenCheckIsValidResourceType_ThenTrueIsReturned(string resourceType)
        {
            Assert.True(_fhirSpecificationProvider.IsValidFhirResourceType(resourceType));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("patient")]
        [InlineData("invalidResourceType")]
        [InlineData("Patient ")]
        public void GivenInvalidResourceType_WhenCheckIsValidResourceType_ThenFalseIsReturned(string resourceType)
        {
            Assert.False(_fhirSpecificationProvider.IsValidFhirResourceType(resourceType));
        }

        [Fact]
        public void GivenValidCompartment_WhenGetResourceTypeByCompartment_ThenTheResourceTypeAreReturned()
        {
            int resourceCount = _fhirSpecificationProvider.GetCompartmentResourceTypes("Patient").Count();
            Assert.Equal(66, resourceCount);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("Account")]
        [InlineData("Device")]
        [InlineData("patient")]
        [InlineData("invalidResourceType")]

        public void GivenInvalidCompartment_WhenGetResourceTypeByCompartment_ExceptionShouldBeThrown(string type)
        {
            Assert.Throws<FhirSpecificationProviderException>(
                () => _fhirSpecificationProvider.GetCompartmentResourceTypes(type));
        }

        [Theory]
        [InlineData("Patient", 29)]
        [InlineData("Account", 14)]
        [InlineData("StructureDefinition", 30)]
        public void GivenValidResourceType_WhenGetSearchParameters_ThenTheSearchParametersAreReturned(string type, int expectedCount)
        {
            int parameterCount = _fhirSpecificationProvider.GetSearchParametersByResourceType(type).Count();
            Assert.Equal(expectedCount, parameterCount);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("invalidResourceType")]
        public void GivenInvalidResourceType_WhenGetSearchParameters_ExceptionShouldBeThrown(string type)
        {
            Assert.Throws<FhirSpecificationProviderException>(
                () => _fhirSpecificationProvider.GetSearchParametersByResourceType(type));
        }


    }
}
