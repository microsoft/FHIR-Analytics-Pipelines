// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Azure.ContainerRegistry;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Core.ContainerRegistry;
using Microsoft.Health.AnalyticsConnector.HealthCheck.Models;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.TemplateManagement.Client;
using Microsoft.Health.Fhir.TemplateManagement.Models;

namespace Microsoft.Health.AnalyticsConnector.HealthCheck.Checkers
{
    public class AzureContainerRegistryHealthChecker : BaseHealthChecker
    {
        private readonly string _imageReference;
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;
        private readonly string _name;

        public AzureContainerRegistryHealthChecker(
            string prefix,
            string imageReference,
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            IDiagnosticLogger diagnosticLogger,
            ILogger<AzureContainerRegistryHealthChecker> logger)
            : base(prefix + HealthCheckTypes.AzureContainerRegistryCanRead, false, diagnosticLogger, logger)
        {
            EnsureArg.IsNotNull(prefix, nameof(prefix));
            _imageReference = EnsureArg.IsNotNull(imageReference, nameof(imageReference));
            _containerRegistryTokenProvider = EnsureArg.IsNotNull(containerRegistryTokenProvider, nameof(containerRegistryTokenProvider));
            _name = prefix + HealthCheckTypes.AzureContainerRegistryCanRead;
        }

        protected override async Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            var healthCheckResult = new HealthCheckResult(_name, false);

            try
            {
                var imageInfo = ImageInfo.CreateFromImageReference(_imageReference);
                string accessToken = await _containerRegistryTokenProvider.GetTokenAsync(imageInfo.Registry, cancellationToken);
                var acrClient = new AzureContainerRegistryClient(imageInfo.Registry, new AcrClientCredentials(accessToken));

                // Ensure we can read from acr.
                await acrClient.Manifests.GetAsync(imageInfo.ImageName, imageInfo.Label, OciArtifactConstants.AcceptedManifestTypes, cancellationToken);
            }
            catch (Exception e)
            {
                Logger.LogInformation(e, $"Health check component {_name}: read ACR {_imageReference} failed: {e}.");
                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = $"Read from acr failed. {e.Message}";
                return healthCheckResult;
            }

            return healthCheckResult;
        }
    }
}
