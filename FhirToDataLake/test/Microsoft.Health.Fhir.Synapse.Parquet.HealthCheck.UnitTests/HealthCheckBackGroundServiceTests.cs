// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests
{
    public class HealthCheckBackgroundServiceTests
    {
        private IHealthCheckEngine _healthCheckEngine;
        private static readonly IDiagnosticLogger DiagnosticLogger = new DiagnosticLogger();

        private readonly NullLogger<HealthCheckBackgroundService> _nullHealthCheckBackgroundServiceLogger =
            NullLogger<HealthCheckBackgroundService>.Instance;

        private static readonly MockHealthCheckMetricsLogger MetricsLogger = new MockHealthCheckMetricsLogger(new NullLogger<MockHealthCheckMetricsLogger>());

        public HealthCheckBackgroundServiceTests()
        {
            var fhirServerHealthChecker = Substitute.For<IHealthChecker>();
            fhirServerHealthChecker.Name.Returns("FhirServer");
            fhirServerHealthChecker.PerformHealthCheckAsync(
                Arg.Any<CancellationToken>()).ReturnsForAnyArgs(
                new HealthCheckResult("FhirServer")
                {
                    Status = HealthCheckStatus.UNHEALTHY,
                });
            var azureBlobStorageHealthChecker = Substitute.For<IHealthChecker>();
            azureBlobStorageHealthChecker.Name.Returns("AzureBlobStorage");
            azureBlobStorageHealthChecker.PerformHealthCheckAsync(
                Arg.Any<CancellationToken>()).ReturnsForAnyArgs(
                new HealthCheckResult("AzureBlobStorage")
                {
                    Status = HealthCheckStatus.HEALTHY,
                });

            var schedulerServiceHealthChecker = Substitute.For<IHealthChecker>();
            schedulerServiceHealthChecker.Name.Returns("SchedulerService");
            schedulerServiceHealthChecker.PerformHealthCheckAsync(
                Arg.Any<CancellationToken>()).ReturnsForAnyArgs(
                new HealthCheckResult("SchedulerService")
                {
                    Status = HealthCheckStatus.UNHEALTHY,
                });
            var filterACRHealthChecker = Substitute.For<IHealthChecker>();
            filterACRHealthChecker.Name.Returns("FilterACR");
            filterACRHealthChecker.PerformHealthCheckAsync(
                Arg.Any<CancellationToken>()).ReturnsForAnyArgs(
                new HealthCheckResult("FilterACR")
                {
                    Status = HealthCheckStatus.HEALTHY,
                });
            var schemaACRHealthChecker = Substitute.For<IHealthChecker>();
            schemaACRHealthChecker.Name.Returns("SchemaACR");
            schemaACRHealthChecker.PerformHealthCheckAsync(
                Arg.Any<CancellationToken>()).ReturnsForAnyArgs(
                new HealthCheckResult("SchemaACR")
                {
                    Status = HealthCheckStatus.UNHEALTHY,
                });
            var healthCheckers = new List<IHealthChecker>() { fhirServerHealthChecker, azureBlobStorageHealthChecker, schedulerServiceHealthChecker, filterACRHealthChecker, schemaACRHealthChecker };
            var healthCheckConfiguration = new HealthCheckConfiguration();
            _healthCheckEngine = new HealthCheckEngine(healthCheckers, Options.Create(healthCheckConfiguration));
        }

        [Fact]
        public async Task WhenAllHealthCheckComplete_MetricsLoggerWillReportCorrectStatus()
        {
            var healthCheckBackgroundService = new HealthCheckBackgroundService(_healthCheckEngine, new List<IHealthCheckListener>() { new MockHealthCheckListener() }, Options.Create(new HealthCheckConfiguration()), DiagnosticLogger, _nullHealthCheckBackgroundServiceLogger, MetricsLogger);

            using var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(TimeSpan.FromSeconds(1));
            await healthCheckBackgroundService.StartAsync(tokenSource.Token);
            Assert.Equal(0, MetricsLogger.HealthStatus["FhirServer"]);
            Assert.Equal(1, MetricsLogger.HealthStatus["AzureBlobStorage"]);
            Assert.Equal(0, MetricsLogger.HealthStatus["SchedulerService"]);
            Assert.Equal(1, MetricsLogger.HealthStatus["FilterACR"]);
            Assert.Equal(0, MetricsLogger.HealthStatus["SchemaACR"]);
        }

        [Fact]
        public async Task WhenErrorOccuredInHealthCheckService_ErrorWillBeReportedInErrorMetrics()
        {
            var brokenHealthCheckBackgroundService = new HealthCheckBackgroundService(_healthCheckEngine, new List<IHealthCheckListener>() { new MockBrokenHealthCheckListener() }, Options.Create(new HealthCheckConfiguration()), DiagnosticLogger, _nullHealthCheckBackgroundServiceLogger, MetricsLogger);

            using var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(TimeSpan.FromSeconds(1));
            await brokenHealthCheckBackgroundService.StartAsync(tokenSource.Token);
            Assert.Equal("HealthCheck", MetricsLogger.ErrorOperationType);
        }
    }
}
