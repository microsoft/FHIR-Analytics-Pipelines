// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests.Checkers
{
    public class FhirServerHealthCheckerTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        [Fact]
        public async Task When_FhirDataClient_CanSearch_HealthCheck_Succeeds()
        {
            var fhirDataClient = Substitute.For<IFhirDataClient>();
            fhirDataClient.SearchAsync(default).Returns(Task.FromResult("result"));
            var fhirServerHealthChecker = new FhirServerHealthChecker(
                fhirDataClient,
                _diagnosticLogger,
                new NullLogger<FhirServerHealthChecker>());

            var result = await fhirServerHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.HEALTHY, result.Status);
        }

        [Fact]
        public async Task When_BlobClient_ThrowExceptionWhenReadWriteABlob_HealthCheck_Fails()
        {
            var fhirDataClient = Substitute.For<IFhirDataClient>();
            fhirDataClient.SearchAsync(default).ThrowsAsyncForAnyArgs(new Exception());
            var fhirServerHealthChecker = new FhirServerHealthChecker(
                fhirDataClient,
                _diagnosticLogger,
                new NullLogger<FhirServerHealthChecker>());

            var result = await fhirServerHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.UNHEALTHY, result.Status);
        }
    }
}
