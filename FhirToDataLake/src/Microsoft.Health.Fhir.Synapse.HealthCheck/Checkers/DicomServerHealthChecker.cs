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
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Api.Dicom;
using Microsoft.Health.Fhir.Synapse.DataClient.Models;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.DicomApiOption;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public class DicomServerHealthChecker : BaseHealthChecker
    {
        private readonly IApiDataClient _dicomDataClient;
        private readonly BaseApiOptions _dicomApiOptions;

        public DicomServerHealthChecker(
            IApiDataClient dicomApiDataClient,
            IDiagnosticLogger diagnosticLogger,
            ILogger<DicomServerHealthChecker> logger)
            : base(HealthCheckTypes.DicomServiceCanRead, false, diagnosticLogger, logger)
        {
            _dicomDataClient = EnsureArg.IsNotNull(dicomApiDataClient, nameof(dicomApiDataClient));

            var queryParameters = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>(DicomApiConstants.IncludeMetadataKey, "false"),
            };

            _dicomApiOptions = new ChangeFeedLatestOptions(queryParameters);
        }

        protected override async Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            var healthCheckResult = new HealthCheckResult(HealthCheckTypes.DicomServiceCanRead);

            try
            {
                // Ensure we can search from DICOM server.
                await _dicomDataClient.SearchAsync(_dicomApiOptions, cancellationToken);
            }
            catch (Exception e)
            {
                Logger.LogInformation(e, $"Health check component {HealthCheckTypes.DicomServiceCanRead}: read DICOM server failed: {e}.");

                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = "Read from DICOM server failed." + e.Message;
                return healthCheckResult;
            }

            healthCheckResult.Status = HealthCheckStatus.HEALTHY;
            return healthCheckResult;
        }
    }
}
