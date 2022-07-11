// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.ContainerRegistry
{
    public class ContainerRegistryTemplateProviderTests
    {
        private readonly string _testContainerRegistryServer;
        private readonly string _testContainerRegistryAccessToken;
        private readonly string _testImageReference;

        public ContainerRegistryTemplateProviderTests()
        {
            _testContainerRegistryServer = Environment.GetEnvironmentVariable("TestContainerRegistryServer");
            if (_testContainerRegistryServer == null)
            {
                return;
            }

            _testImageReference = $"{_testContainerRegistryServer}/synapsetesttemplates:latest";

            var testContainerRegistryUsername = Environment.GetEnvironmentVariable("TestContainerRegistryServer")?.Split('.')[0];
            var testContainerRegistryPassword = Environment.GetEnvironmentVariable("TestContainerRegistryPassword");
            Console.WriteLine($"Password length: {testContainerRegistryPassword.Length}");

            _testContainerRegistryAccessToken = GetContainerRegistryAccessToken(testContainerRegistryUsername, testContainerRegistryPassword);
        }

        [SkippableFact]
        public async Task GivenTemplateReference_WhenFetchingTemplates_CorrectTemplateCollectionsShouldBeReturned()
        {
            Skip.If(_testContainerRegistryServer == null);

            ImageInfo imageInfo = ImageInfo.CreateFromImageReference(_testImageReference);
            await ContainerRegistryTestUtils.GenerateTemplateImageAsync(imageInfo, _testContainerRegistryAccessToken, TestConstants.TestTemplateTarGzPath);

            var containerRegistryTemplateProvider = GetTestTemplateProvider(_testContainerRegistryAccessToken);
            var templateCollection = await containerRegistryTemplateProvider.GetTemplateCollectionAsync(_testImageReference, CancellationToken.None);

            Assert.NotEmpty(templateCollection);
        }

        [SkippableFact]
        public async Task GivenAnInvalidToken_WhenFetchingTemplates_ExceptionShouldBeThrown()
        {
            Skip.If(_testContainerRegistryServer == null);
            var containerRegistryTemplateProvider = GetTestTemplateProvider("invalid token");

            await Assert.ThrowsAsync<ContainerRegistrySchemaException>(() => containerRegistryTemplateProvider.GetTemplateCollectionAsync(_testImageReference, CancellationToken.None));
        }

        private IContainerRegistryTemplateProvider GetTestTemplateProvider(string accessToken)
        {
            IContainerRegistryTokenProvider tokenProvider = Substitute.For<IContainerRegistryTokenProvider>();
            tokenProvider.GetTokenAsync(default, default).ReturnsForAnyArgs($"Basic {accessToken}");

            return new ContainerRegistryTemplateProvider(tokenProvider, new NullLogger<ContainerRegistryTemplateProvider>());
        }

        private string GetContainerRegistryAccessToken(string serverUsername, string serverPassword)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes($"{serverUsername}:{serverPassword}"));
        }
    }
}
