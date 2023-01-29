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
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Fhir;
using Microsoft.Health.AnalyticsConnector.DataClient.Models.FhirApiOption;
using Microsoft.Health.AnalyticsConnector.HealthCheck.Models;

namespace Microsoft.Health.AnalyticsConnector.HealthCheck.Checkers
{
    public class FhirServerHealthChecker : BaseHealthChecker
    {
        private const string SampleResourceType = "Patient";
        private readonly IApiDataClient _fhirApiDataClient;
        private readonly BaseSearchOptions _searchOptions;

        public FhirServerHealthChecker(
            IApiDataClient fhirApiDataClient,
            IDiagnosticLogger diagnosticLogger,
            ILogger<FhirServerHealthChecker> logger)
            : base(HealthCheckTypes.FhirServiceCanRead, false, diagnosticLogger, logger)
        {
            _fhirApiDataClient = EnsureArg.IsNotNull(fhirApiDataClient, nameof(fhirApiDataClient));

            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Single.ToString("d")),
            };

            _searchOptions = new BaseSearchOptions(SampleResourceType, queryParameters);
        }

        protected override async Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            var healthCheckResult = new HealthCheckResult(HealthCheckTypes.FhirServiceCanRead);

            try
            {
                // Ensure we can search from FHIR server.
                await _fhirApiDataClient.SearchAsync(_searchOptions, cancellationToken);
            }
            catch (Exception e)
            {
                Logger.LogInformation(e, $"Health check component {HealthCheckTypes.FhirServiceCanRead}: read FHIR server failed: {e}.");

                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = "Read from FHIR server failed." + e.Message;
                return healthCheckResult;
            }

            healthCheckResult.Status = HealthCheckStatus.HEALTHY;
            return healthCheckResult;
        }
    }
}
