// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Castle.Core.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.ContainerRegistry
{
    public class ContainerRegistryTemplateProviderTests
    {
        private readonly string _testImageReference;
        private readonly string _testContainerRegistryAccessToken;
        private readonly IContainerRegistryTokenProvider _testTokenProvider;

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

            _testContainerRegistryAccessToken = GetTestAccessToken(testContainerRegistryUsername, testContainerRegistryPassword);
            _testTokenProvider = GetTestAcrTokenProvider(_testContainerRegistryAccessToken);
        }

        [SkippableFact]
        public async Task GivenTemplateReference_WhenFetchingTemplates_CorrectTemplateCollectionsShouldBeReturned()
        {
            Skip.If(_testImageReference == null);

            ImageInfo imageInfo = ImageInfo.CreateFromImageReference(_testImageReference);
            await ContainerRegistryTestUtils.GenerateTemplateImageAsync(imageInfo, _testContainerRegistryAccessToken, TestConstants.TestTemplateTarGzPath);

            var containerRegistryTemplateProvider = new ContainerRegistryTemplateProvider(
                _testTokenProvider,
                Options.Create(new ContainerRegistryConfiguration() { SchemaImageReference = _testImageReference }),
                new NullLogger<ContainerRegistryTemplateProvider>());
            var templateCollection = await containerRegistryTemplateProvider.GetTemplateCollectionAsync(CancellationToken.None);

            Assert.NotEmpty(templateCollection);
        }

        [SkippableFact]
        public async Task GivenAnInvalidToken_WhenFetchingTemplates_ExceptionShouldBeThrown()
        {
            Skip.If(_testImageReference == null);

            var containerRegistryTemplateProvider = new ContainerRegistryTemplateProvider(
                GetTestAcrTokenProvider("invalid token"),
                Options.Create(new ContainerRegistryConfiguration() { SchemaImageReference = _testImageReference }),
                new NullLogger<ContainerRegistryTemplateProvider>());

            await Assert.ThrowsAsync<ContainerRegistrySchemaException>(() => containerRegistryTemplateProvider.GetTemplateCollectionAsync(CancellationToken.None));
        }

        private IContainerRegistryTokenProvider GetTestAcrTokenProvider(string accessToken)
        {
            var tokenProvider = Substitute.For<IContainerRegistryTokenProvider>();

            tokenProvider.GetTokenAsync(default, default).ReturnsForAnyArgs($"Basic {accessToken}");
            return tokenProvider;
        }

        private string GetTestAccessToken(string serverUsername, string serverPassword)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes($"{serverUsername}:{serverPassword}"));
        }
    }
}
