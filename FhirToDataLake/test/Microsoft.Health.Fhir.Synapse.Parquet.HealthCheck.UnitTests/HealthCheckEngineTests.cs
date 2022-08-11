// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
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
                    Status = HealthCheckStatus.FAIL,
                });
            _azureBlobStorageHealthChecker = Substitute.For<IHealthChecker>();
            _azureBlobStorageHealthChecker.Name.Returns("AzureBlobStorage");
            _azureBlobStorageHealthChecker.PerformHealthCheckAsync(
                Arg.Any<CancellationToken>()).ReturnsForAnyArgs(
                new HealthCheckResult("AzureBlobStorage")
                {
                    Status = HealthCheckStatus.PASS,
                });
        }

        [Fact]
        public async Task When_All_HealthCheck_Complete_All_AreMaked_WithCorrectStatus()
        {
            var healthCheckers = new List<IHealthChecker>() { _fhirServerHealthChecker, _azureBlobStorageHealthChecker };
            var healthCheckConfiduration = new HealthCheckConfiguration();
            var healthCheckEngine = new HealthCheckEngine(healthCheckers, Options.Create(healthCheckConfiduration), new NullLogger<HealthCheckEngine>());

            var healthStatus = new HealthStatus();
            await healthCheckEngine.CheckHealthAsync(healthStatus);
            var sortedHealthCheckResults = healthStatus.HealthCheckResults.OrderBy(h => h.Name);
            Assert.Collection(
                sortedHealthCheckResults,
                p =>
                {
                    Assert.Equal("AzureBlobStorage", p.Name);
                    Assert.Equal(HealthCheckStatus.PASS, p.Status);
                },
                p =>
                {
                    Assert.Equal("FhirServer", p.Name);
                    Assert.Equal(HealthCheckStatus.FAIL, p.Status);
                });
        }

        [Fact]
        public async Task When_HealthCheck_ExceedsHealthCheckTimeLimit_HealthCheck_MarkedAsFailed()
        {
            var healthCheckConfiguration = new HealthCheckConfiguration();
            healthCheckConfiguration.HealthCheckTimeoutInSeconds = 0.1;

            var mockTimeOutHealthChecker = new MockTimeoutHealthChecker(new NullLogger<MockTimeoutHealthChecker>());
            var healthCheckers = new List<IHealthChecker>() { _fhirServerHealthChecker, _azureBlobStorageHealthChecker, mockTimeOutHealthChecker };
            var healthCheckEngine = new HealthCheckEngine(healthCheckers, Options.Create(healthCheckConfiguration), new NullLogger<HealthCheckEngine>());

            var healthStatus = new HealthStatus();
            await healthCheckEngine.CheckHealthAsync(healthStatus);
            var sortedHealthCheckResults = healthStatus.HealthCheckResults.OrderBy(h => h.Name);
            Assert.Collection(
                sortedHealthCheckResults,
                p =>
                {
                    Assert.Equal("AzureBlobStorage", p.Name);
                    Assert.Equal(HealthCheckStatus.PASS, p.Status);
                },
                p =>
                {
                    Assert.Equal("FhirServer", p.Name);
                    Assert.Equal(HealthCheckStatus.FAIL, p.Status);
                },
                p =>
                {
                    Assert.Equal("MockTimeout", p.Name);
                    Assert.Equal(HealthCheckStatus.FAIL, p.Status);
                });
        }
    }
}
