// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataFilter
{
    public class ContainerRegistryFilterProviderTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private readonly string _testImageReference;
        private readonly string _testContainerRegistryAccessToken;
        private readonly IContainerRegistryTokenProvider _testTokenProvider;

        public ContainerRegistryFilterProviderTests()
        {
            string testContainerRegistryServer = Environment.GetEnvironmentVariable("TestContainerRegistryServer");
            if (testContainerRegistryServer == null)
            {
                return;
            }

            _testImageReference = $"{testContainerRegistryServer}/synapsetestfilter:latest";

            string testContainerRegistryUsername = Environment.GetEnvironmentVariable("TestContainerRegistryServer")?.Split('.')[0];
            string testContainerRegistryPassword = Environment.GetEnvironmentVariable("TestContainerRegistryPassword");

            _testContainerRegistryAccessToken = ContainerRegistryTestUtils.GetAcrAccessToken(testContainerRegistryUsername, testContainerRegistryPassword);
            _testTokenProvider = ContainerRegistryTestUtils.GetMockAcrTokenProvider(_testContainerRegistryAccessToken);
        }

        [SkippableFact]
        public async Task GivenFilterReference_WhenFetchingFilterConfig_CorrectConfigShouldBeReturned()
        {
            Skip.If(_testImageReference == null);

            ImageInfo imageInfo = ImageInfo.CreateFromImageReference(_testImageReference);
            await ContainerRegistryTestUtils.GenerateImageAsync(imageInfo, _testContainerRegistryAccessToken, TestUtils.TestFilterTarGzPath);

            var containerRegistryFilterProvider = new ContainerRegistryFilterProvider(
                Options.Create(new FilterLocation()
                {
                    EnableExternalFilter = true,
                    FilterImageReference = _testImageReference,
                }),
                _testTokenProvider,
                _diagnosticLogger,
                new NullLogger<ContainerRegistryFilterProvider>());
            FilterConfiguration filterConfiguration = await containerRegistryFilterProvider.GetFilterAsync(CancellationToken.None);

            Assert.Equal(Common.Models.Jobs.FilterScope.System, filterConfiguration.FilterScope);
            Assert.Equal("test", filterConfiguration.TypeFilters);
            Assert.Equal("test", filterConfiguration.RequiredTypes);
            Assert.Equal("123", filterConfiguration.GroupId);
        }

        [SkippableFact]
        public async Task GivenAnInvalidToken_WhenFetchingFilter_ExceptionShouldBeThrown()
        {
            Skip.If(_testImageReference == null);

            var containerRegistryFilterProvider = new ContainerRegistryFilterProvider(
                Options.Create(new FilterLocation()
                {
                    EnableExternalFilter = true,
                    FilterImageReference = _testImageReference,
                }),
                ContainerRegistryTestUtils.GetMockAcrTokenProvider("invalid token"),
                _diagnosticLogger,
                new NullLogger<ContainerRegistryFilterProvider>());

            await Assert.ThrowsAsync<ContainerRegistryFilterException>(
                () => containerRegistryFilterProvider.GetFilterAsync(CancellationToken.None));
        }
    }
}
