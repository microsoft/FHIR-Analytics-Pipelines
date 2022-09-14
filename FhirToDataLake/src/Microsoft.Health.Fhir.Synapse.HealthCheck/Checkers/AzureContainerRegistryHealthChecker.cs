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
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.TemplateManagement.Client;
using Microsoft.Health.Fhir.TemplateManagement.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public class AzureContainerRegistryHealthChecker : BaseHealthChecker
    {
        private const string MediatypeV2Manifest = "application/vnd.docker.distribution.manifest.v2+json";
        private readonly string _imageReference;
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;

        public AzureContainerRegistryHealthChecker(
            IOptions<SchemaConfiguration> schemaConfiguration,
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            ILogger<AzureContainerRegistryHealthChecker> logger)
            : base(HealthCheckTypes.AzureContainerRegistryCanRead, false, logger)
        {
            EnsureArg.IsNotNull(schemaConfiguration, nameof(schemaConfiguration));
            _containerRegistryTokenProvider = EnsureArg.IsNotNull(containerRegistryTokenProvider, nameof(containerRegistryTokenProvider));

            _imageReference = schemaConfiguration.Value.SchemaImageReference;
        }

        protected override async Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            var healthCheckResult = new HealthCheckResult(HealthCheckTypes.AzureContainerRegistryCanRead, false);

            try
            {
                var imageInfo = ImageInfo.CreateFromImageReference(_imageReference);
                var accessToken = await _containerRegistryTokenProvider.GetTokenAsync(imageInfo.Registry, cancellationToken);
                var acrClient = new AzureContainerRegistryClient(imageInfo.Registry, new AcrClientCredentials(accessToken));

                // Ensure we can read from acr.
                await acrClient.Manifests.GetAsync(imageInfo.ImageName, imageInfo.Label, MediatypeV2Manifest, cancellationToken);
            }
            catch (Exception e)
            {
                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = $"Read from acr failed. {e.Message}";
                return healthCheckResult;
            }

            return healthCheckResult;
        }
    }
}
