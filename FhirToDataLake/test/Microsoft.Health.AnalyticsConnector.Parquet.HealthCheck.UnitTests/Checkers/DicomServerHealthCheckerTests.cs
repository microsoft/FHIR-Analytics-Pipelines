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
    public class DicomServerHealthCheckerTests
    {
        private static readonly IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new DicomServerHealthChecker(null, _diagnosticLogger, new NullLogger<DicomServerHealthChecker>()));
        }

        [Fact]
        public async Task GivenValidDataClient_WhenCanSearch_HealthCheckShouldSucceed()
        {
            var dicomDataClient = Substitute.For<IApiDataClient>();
            dicomDataClient.SearchAsync(default).Returns(Task.FromResult("result"));

            var dicomServerHealthChecker = new DicomServerHealthChecker(
                dicomDataClient,
                _diagnosticLogger,
                new NullLogger<DicomServerHealthChecker>());

            HealthCheckResult result = await dicomServerHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.HEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }

        [Fact]
        public async Task GivenValidDataClient_WhenThrow_HealthCheckShouldFail()
        {
            var dicomDataClient = Substitute.For<IApiDataClient>();
            dicomDataClient.SearchAsync(default).ThrowsAsyncForAnyArgs(new Exception());
            var dicomServerHealthChecker = new DicomServerHealthChecker(
                dicomDataClient,
                _diagnosticLogger,
                new NullLogger<DicomServerHealthChecker>());

            HealthCheckResult result = await dicomServerHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.UNHEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }
    }
}
