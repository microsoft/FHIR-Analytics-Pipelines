// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.UnitTests;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests.Checkers
{
    public class SchemaAzureContainerRegistryHealthCheckerTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private static ILogger<SchemaAzureContainerRegistryHealthChecker> _logger = new NullLogger<SchemaAzureContainerRegistryHealthChecker>();

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new SchemaAzureContainerRegistryHealthChecker(null, null, _diagnosticLogger, _logger));
        }

        [Fact]
        public async Task When_InputImageReferenceInvalid_HealthCheck_Fails()
        {
            var schemaConfiguration = new SchemaConfiguration()
            {
                SchemaImageReference = "test",
            };

            IContainerRegistryTokenProvider tokenProvider = Substitute.For<IContainerRegistryTokenProvider>();

            var schemaACRHealthChecker = new SchemaAzureContainerRegistryHealthChecker(Options.Create(schemaConfiguration), tokenProvider, _diagnosticLogger, _logger);
            var result = await schemaACRHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.UNHEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }

        [Fact]
        public async Task When_InputImageReferenceNotFound_HealthCheck_Fails()
        {
            var schemaConfiguration = new SchemaConfiguration()
            {
                SchemaImageReference = "test",
            };

            IContainerRegistryTokenProvider tokenProvider = Substitute.For<IContainerRegistryTokenProvider>();

            var schemaACRHealthChecker = new SchemaAzureContainerRegistryHealthChecker(Options.Create(schemaConfiguration), tokenProvider, _diagnosticLogger, _logger);
            var result = await schemaACRHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.UNHEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }

        [SkippableFact]
        public async Task When_GivenValidACR_HealthCheck_Succeeds()
        {
            var testContainerRegistryServer = Environment.GetEnvironmentVariable("TestContainerRegistryServer");
            Skip.If(testContainerRegistryServer == null);

            var testImageReference = $"{testContainerRegistryServer}/synapsetestschema:testhealthcheck";

            var testContainerRegistryUsername = Environment.GetEnvironmentVariable("TestContainerRegistryServer")?.Split('.')[0];
            var testContainerRegistryPassword = Environment.GetEnvironmentVariable("TestContainerRegistryPassword");

            var testContainerRegistryAccessToken = ContainerRegistryTestUtils.GetAcrAccessToken(testContainerRegistryUsername, testContainerRegistryPassword);
            var testTokenProvider = ContainerRegistryTestUtils.GetMockAcrTokenProvider(testContainerRegistryAccessToken);

            var schemaConfiguration = new SchemaConfiguration()
            {
                SchemaImageReference = testImageReference,
            };

            var schemaACRHealthChecker = new SchemaAzureContainerRegistryHealthChecker(Options.Create(schemaConfiguration), testTokenProvider, _diagnosticLogger, _logger);

            try
            {
                ImageInfo imageInfo = ImageInfo.CreateFromImageReference(testImageReference);
                await ContainerRegistryTestUtils.GenerateImageAsync(imageInfo, testContainerRegistryAccessToken, TestUtils.TestFilterTarGzPath);
            }
            catch
            {
                var failedResult = await schemaACRHealthChecker.PerformHealthCheckAsync();
                Assert.Equal(HealthCheckStatus.UNHEALTHY, failedResult.Status);
                Assert.False(failedResult.IsCritical);
            }

            var result = await schemaACRHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.HEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }
    }
}
