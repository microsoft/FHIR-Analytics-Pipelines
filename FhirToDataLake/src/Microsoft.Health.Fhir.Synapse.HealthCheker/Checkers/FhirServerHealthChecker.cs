// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Checkers
{
    public class FhirServerHealthChecker : BaseHealthChecker
    {
        private const string SampleResourceType = "Patient";
        private const string SampleStartTime = "2021-08-01T12:00:00+08:00";
        private const string SampleEndTime = "2021-08-09T12:40:59+08:00";
        private readonly IFhirDataClient _fhirApiDataClient;

        public FhirServerHealthChecker(
            IFhirDataClient fhirApiDataClient,
            ILogger<FhirServerHealthChecker> logger)
            : base(HealthCheckTypes.FhirServiceCanRead, logger)
        {
            EnsureArg.IsNotNull(fhirApiDataClient, nameof(fhirApiDataClient));

            _fhirApiDataClient = fhirApiDataClient;
        }

        protected override async Task PerformHealthCheckImplAsync(HealthCheckResult healthCheckResult, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(healthCheckResult, nameof(healthCheckResult));

            // Ensure we can search from FHIR server.
            var searchParameters = new FhirSearchParameters(SampleResourceType, DateTimeOffset.Parse(SampleStartTime), DateTimeOffset.Parse(SampleEndTime), string.Empty);

            await _fhirApiDataClient.SearchAsync(searchParameters, cancellationToken);

            healthCheckResult.Status = HealthCheckStatus.PASS;
        }
    }
}
