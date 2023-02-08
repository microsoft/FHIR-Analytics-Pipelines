// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.HealthCheck.Checkers;
using Microsoft.Health.AnalyticsConnector.HealthCheck.Models;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.HealthCheck.UnitTests.Checkers
{
    public class FhirServerHealthCheckerTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new FhirServerHealthChecker(null, _diagnosticLogger, new NullLogger<FhirServerHealthChecker>()));
        }

        [Fact]
        public async Task When_FhirDataClient_CanSearch_HealthCheck_Succeeds()
        {
            var fhirDataClient = Substitute.For<IApiDataClient>();
            fhirDataClient.SearchAsync(default).Returns(Task.FromResult("result"));
            var fhirServerHealthChecker = new FhirServerHealthChecker(
                fhirDataClient,
                _diagnosticLogger,
                new NullLogger<FhirServerHealthChecker>());

            HealthCheckResult result = await fhirServerHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.HEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }

        [Fact]
        public async Task When_FhirDataClient_ThrowExceptionWhenSearch_HealthCheck_Fails()
        {
            var fhirDataClient = Substitute.For<IApiDataClient>();
            fhirDataClient.SearchAsync(default).ThrowsAsyncForAnyArgs(new Exception());
            var fhirServerHealthChecker = new FhirServerHealthChecker(
                fhirDataClient,
                _diagnosticLogger,
                new NullLogger<FhirServerHealthChecker>());

            HealthCheckResult result = await fhirServerHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.UNHEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }
    }
}
