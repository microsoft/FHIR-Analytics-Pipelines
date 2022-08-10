// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Extensions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public class FhirServerHealthChecker : BaseHealthChecker
    {
        private const string SampleResourceType = "Patient";
        private const string SampleStartTime = "2021-08-01T12:00:00+08:00";
        private const string SampleEndTime = "2021-08-09T12:40:59+08:00";
        private readonly IFhirDataClient _fhirApiDataClient;
        private readonly BaseSearchOptions _searchOptions;

        public FhirServerHealthChecker(
            IFhirDataClient fhirApiDataClient,
            ILogger<FhirServerHealthChecker> logger)
            : base(HealthCheckTypes.FhirServiceCanRead, logger)
        {
            EnsureArg.IsNotNull(fhirApiDataClient, nameof(fhirApiDataClient));

            _fhirApiDataClient = fhirApiDataClient;

            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new (FhirApiConstants.LastUpdatedKey, $"ge{DateTimeOffset.Parse(SampleStartTime).ToInstantString()}"),
                new (FhirApiConstants.LastUpdatedKey, $"lt{DateTimeOffset.Parse(SampleEndTime).ToInstantString()}"),
            };

            _searchOptions = new BaseSearchOptions(SampleResourceType, queryParameters);
        }

        protected override async Task PerformHealthCheckImplAsync(HealthCheckResult healthCheckResult, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(healthCheckResult, nameof(healthCheckResult));

            // Ensure we can search from FHIR server.
            await _fhirApiDataClient.SearchAsync(_searchOptions, cancellationToken);
        }
    }
}
