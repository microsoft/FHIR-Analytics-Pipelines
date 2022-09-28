// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Fhir
{
    public class R4FhirSpecificationProviderTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        private IFhirSpecificationProvider _r4FhirSpecificationProvider;

        private readonly CancellationToken _noneCancellationToken = CancellationToken.None;
        private readonly NullLogger<R4FhirSpecificationProvider> _nullR4FhirSpecificationProviderLogger =
            NullLogger<R4FhirSpecificationProvider>.Instance;

        public R4FhirSpecificationProviderTests()
        {
            var dataClient = Substitute.For<IFhirDataClient>();

            var metadataOptions = new MetadataOptions();
            dataClient.Search(metadataOptions)
                .ReturnsForAnyArgs(x => TestDataProvider.GetBundleFromFile(TestDataConstants.MetadataFile));

            _r4FhirSpecificationProvider = new R4FhirSpecificationProvider(dataClient, _diagnosticLogger, _nullR4FhirSpecificationProviderLogger);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new R4FhirSpecificationProvider(null, _diagnosticLogger, _nullR4FhirSpecificationProviderLogger));

            var dataClient = Substitute.For<IFhirDataClient>();

            Assert.Throws<ArgumentNullException>(
                () => new R4FhirSpecificationProvider(dataClient, _diagnosticLogger, null));
        }

        [Fact]
        public void GivenBrokenDataClient_WhenInitialize_ExceptionShouldBeThrown()
        {
            var dataClient = Substitute.For<IFhirDataClient>();
            dataClient.SearchAsync(default).ThrowsForAnyArgs(new FhirSearchException("mockException"));
            Assert.Throws<FhirSpecificationProviderException>(
                () => new R4FhirSpecificationProvider(dataClient, _diagnosticLogger, _nullR4FhirSpecificationProviderLogger));
        }

        [Theory]
        [InlineData("{\"invalidCapabilityStatement\": 0}")]
        [InlineData("{\"resourceType\": \"CapabilityStatement\"}")]
        [InlineData("{\"resourceType\": \"CapabilityStatement\",\"rest\": []}")]
        [InlineData("{\"resourceType\": \"CapabilityStatement\",\"rest\": [{\"mode\":\"server\"}]}")]
        public void GivenInvalidMetadata_WhenInitialize_ExceptionShouldBeThrown(string metadataContent)
        {
            var dataClient = Substitute.For<IFhirDataClient>();
            dataClient.SearchAsync(default).ReturnsForAnyArgs(metadataContent);

            Assert.Throws<FhirSpecificationProviderException>(
                () => new R4FhirSpecificationProvider(dataClient, _diagnosticLogger, _nullR4FhirSpecificationProviderLogger));
        }

        [Fact]
        public void WhenGetAllResourceTypes_TheResourcesTypeShouldBeReturned()
        {
            var types = _r4FhirSpecificationProvider.GetAllResourceTypes().ToList();
            Assert.Equal(145, types.Count);
        }

        [Theory]
        [InlineData("Patient")]
        [InlineData("Account")]
        public void GivenValidResourceType_WhenCheckIsFhirResourceType_TrueShouldBeReturned(string type)
        {
            var isValid = _r4FhirSpecificationProvider.IsValidFhirResourceType(type);
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
            var isValid = _r4FhirSpecificationProvider.IsValidFhirResourceType(type);
            Assert.False(isValid);
        }

        [Fact]
        public void GivenValidCompartmentType_WhenGetCompartmentResourceTypes_ResourceTypesShouldBeReturned()
        {
            var types = _r4FhirSpecificationProvider.GetCompartmentResourceTypes("Patient");
            Assert.Equal(66, types.Count());
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
            Assert.Throws<FhirSpecificationProviderException>(() => _r4FhirSpecificationProvider.GetCompartmentResourceTypes(type));
        }

        [Theory]
        [InlineData("Patient", 29)]
        [InlineData("Account", 14)]
        public void GivenValidResourceType_WhenGetSearchParametersByResourceType_SearchParametersShouldBeReturned(
            string type,
            int cnt)
        {
            var parameters = _r4FhirSpecificationProvider.GetSearchParametersByResourceType(type).ToList();
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
            Assert.Throws<FhirSpecificationProviderException>(() => _r4FhirSpecificationProvider.GetSearchParametersByResourceType(type));
        }
    }
}
