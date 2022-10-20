// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests
{
    public class HealthCheckEngineTests
    {
        private IHealthChecker _fhirServerHealthChecker;
        private IHealthChecker _azureBlobStorageHealthChecker;

        public HealthCheckEngineTests()
        {
            _fhirServerHealthChecker = Substitute.For<IHealthChecker>();
            _fhirServerHealthChecker.Name.Returns("FhirServer");
            _fhirServerHealthChecker.PerformHealthCheckAsync(
                Arg.Any<CancellationToken>()).ReturnsForAnyArgs(
                new HealthCheckResult("FhirServer")
                {
                    Status = HealthCheckStatus.UNHEALTHY,
                });
            _azureBlobStorageHealthChecker = Substitute.For<IHealthChecker>();
            _azureBlobStorageHealthChecker.Name.Returns("AzureBlobStorage");
            _azureBlobStorageHealthChecker.PerformHealthCheckAsync(
                Arg.Any<CancellationToken>()).ReturnsForAnyArgs(
                new HealthCheckResult("AzureBlobStorage")
                {
                    Status = HealthCheckStatus.HEALTHY,
                });
        }

        [Fact]
        public async Task When_All_HealthCheck_Complete_All_AreMaked_WithCorrectStatus()
        {
            var healthCheckers = new List<IHealthChecker>() { _fhirServerHealthChecker, _azureBlobStorageHealthChecker };
            var healthCheckConfiduration = new HealthCheckConfiguration();
            var healthCheckEngine = new HealthCheckEngine(healthCheckers, Options.Create(healthCheckConfiduration));

            var healthStatus = await healthCheckEngine.CheckHealthAsync();
            var sortedHealthCheckResults = healthStatus.HealthCheckResults.OrderBy(h => h.Name);
            var expectedResult = JsonSerializer.Deserialize<List<HealthCheckResult>>(File.ReadAllText("TestData/result.txt"));
            Assert.Collection(
                sortedHealthCheckResults,
                p =>
                {
                    Assert.Equal(expectedResult[0].Name, p.Name);
                    Assert.Equal(expectedResult[0].Status, p.Status);
                },
                p =>
                {
                    Assert.Equal(expectedResult[1].Name, p.Name);
                    Assert.Equal(expectedResult[1].Status, p.Status);
                });
            Assert.Equal(HealthCheckStatus.HEALTHY, healthStatus.Status);
        }

        [Fact]
        public async Task When_HealthCheck_ExceedsHealthCheckTimeLimit_HealthCheck_MarkedAsFailed()
        {
            var healthCheckConfiguration = new HealthCheckConfiguration();
            healthCheckConfiguration.HealthCheckTimeoutInSeconds = 1;

            var mockTimeOutHealthChecker = new MockTimeoutHealthChecker(new DiagnosticLogger() ,new NullLogger<MockTimeoutHealthChecker>());
            var healthCheckers = new List<IHealthChecker>() { _fhirServerHealthChecker, _azureBlobStorageHealthChecker, mockTimeOutHealthChecker };
            var healthCheckEngine = new HealthCheckEngine(healthCheckers, Options.Create(healthCheckConfiguration));

            var healthStatus = await healthCheckEngine.CheckHealthAsync();
            var sortedHealthCheckResults = healthStatus.HealthCheckResults.OrderBy(h => h.Name);
            Assert.Collection(
                sortedHealthCheckResults,
                p =>
                {
                    Assert.Equal("AzureBlobStorage", p.Name);
                    Assert.Equal(HealthCheckStatus.HEALTHY, p.Status);
                },
                p =>
                {
                    Assert.Equal("FhirServer", p.Name);
                    Assert.Equal(HealthCheckStatus.UNHEALTHY, p.Status);
                },
                p =>
                {
                    Assert.Equal("MockTimeout", p.Name);
                    Assert.Equal(HealthCheckStatus.UNHEALTHY, p.Status);
                });
            Assert.Equal(HealthCheckStatus.UNHEALTHY, healthStatus.Status);
        }
    }
}
