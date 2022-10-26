// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.UnitTests;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.ContainerRegistry
{
    public class ContainerRegistryTemplateProviderTests
    {
        private readonly string _testImageReference;
        private readonly string _testContainerRegistryAccessToken;
        private readonly IContainerRegistryTokenProvider _testTokenProvider;

        private readonly IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private readonly ILogger<ContainerRegistryTemplateProvider> _nullLogger = new NullLogger<ContainerRegistryTemplateProvider>();

        public ContainerRegistryTemplateProviderTests()
        {
            var testContainerRegistryServer = Environment.GetEnvironmentVariable("TestContainerRegistryServer");
            if (testContainerRegistryServer == null)
            {
                return;
            }

            _testImageReference = $"{testContainerRegistryServer}/synapsetesttemplates:latest";

            var testContainerRegistryUsername = Environment.GetEnvironmentVariable("TestContainerRegistryServer")?.Split('.')[0];
            var testContainerRegistryPassword = Environment.GetEnvironmentVariable("TestContainerRegistryPassword");

            _testContainerRegistryAccessToken = TestUtils.GetAcrAccessToken(testContainerRegistryUsername, testContainerRegistryPassword);
            _testTokenProvider = TestUtils.GetMockAcrTokenProvider(_testContainerRegistryAccessToken);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new ContainerRegistryTemplateProvider(null, _diagnosticLogger, _nullLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ContainerRegistryTemplateProvider(_testTokenProvider, null, _nullLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ContainerRegistryTemplateProvider(_testTokenProvider, _diagnosticLogger, null));
        }

        [SkippableFact]
        public async Task GivenTemplateReference_WhenFetchingTemplates_CorrectTemplateCollectionsShouldBeReturned()
        {
            Skip.If(_testImageReference == null);

            ImageInfo imageInfo = ImageInfo.CreateFromImageReference(_testImageReference);
            await ContainerRegistryTestUtils.GenerateImageAsync(imageInfo, _testContainerRegistryAccessToken, TestUtils.TestTemplateTarGzPath);

            var containerRegistryTemplateProvider = new ContainerRegistryTemplateProvider(_testTokenProvider, _diagnosticLogger, _nullLogger);
            var templateCollection = await containerRegistryTemplateProvider.GetTemplateCollectionAsync(_testImageReference, CancellationToken.None);

            Assert.NotEmpty(templateCollection);
        }

        [SkippableTheory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData("Invalid reference")]
        [InlineData(null)]
        public async Task GivenInvalidSchemaReference_WhenFetchingTemplates_CorrectTemplateCollectionsShouldBeReturned(string imageReference)
        {
            Skip.If(_testImageReference == null);

            var containerRegistryTemplateProvider = new ContainerRegistryTemplateProvider(_testTokenProvider, _diagnosticLogger, _nullLogger);
            await Assert.ThrowsAsync<ContainerRegistrySchemaException>(
                () => containerRegistryTemplateProvider.GetTemplateCollectionAsync(imageReference, CancellationToken.None));
        }

        [SkippableFact]
        public async Task GivenAnInvalidToken_WhenFetchingTemplates_ExceptionShouldBeThrown()
        {
            Skip.If(_testImageReference == null);

            var containerRegistryTemplateProvider = new ContainerRegistryTemplateProvider(TestUtils.GetMockAcrTokenProvider("invalid token"), _diagnosticLogger, _nullLogger);
            await Assert.ThrowsAsync<ContainerRegistrySchemaException>(
                () => containerRegistryTemplateProvider.GetTemplateCollectionAsync(_testImageReference, CancellationToken.None));
        }
    }
}
