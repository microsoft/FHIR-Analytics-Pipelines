// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using EnsureThat;
using Microsoft.Azure.ContainerRegistry;
using Microsoft.Build.Framework;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Microsoft.Health.Fhir.TemplateManagement.Client;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using Microsoft.Win32;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public class AzureContainerRegistryHealthChecker : BaseHealthChecker
    {
        public const string HealthCheckBlobPrefix = ".healthcheck";
        public const string HealthCheckUploadedContent = "healthCheckContent";
        public const string MediatypeV2Manifest = "application/vnd.docker.distribution.manifest.v2+json";
        private readonly ParquetSchemaProviderDelegate _parquetSchemaDelegate;
        private readonly SchemaConfiguration _schemaConfiguration;
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;

        public AzureContainerRegistryHealthChecker(
            IOptions<SchemaConfiguration> schemaConfiguration,
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            ILogger<AzureBlobStorageHealthChecker> logger)
            : base(HealthCheckTypes.AzureBlobStorageCanReadWrite, false, logger)
        {
            EnsureArg.IsNotNull(schemaConfiguration, nameof(schemaConfiguration));
            _containerRegistryTokenProvider = EnsureArg.IsNotNull(containerRegistryTokenProvider, nameof(containerRegistryTokenProvider));

            _schemaConfiguration = schemaConfiguration.Value;
        }

        protected override async Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            if (!_schemaConfiguration.EnableCustomizedSchema)
            {
            }

            var imageReference = _schemaConfiguration.SchemaImageReference;
            var imageInfo = ImageInfo.CreateFromImageReference(imageReference);
            var accessToken = await _containerRegistryTokenProvider.GetTokenAsync(imageInfo.Registry, cancellationToken);

            var acrClient = new AzureContainerRegistryClient(imageInfo.Registry, new AcrClientCredentials(accessToken));

            var healthCheckResult = new HealthCheckResult(HealthCheckTypes.AzureBlobStorageCanReadWrite, false);
            var blobPath = HealthCheckBlobPrefix;

            try
            {
                var provider = 
                // Ensure we can write to the storage account
                await acrClient.Manifests.GetAsync(imageInfo.ImageName, imageInfo.Label, MediatypeV2Manifest, cancellationToken);
            }
            catch (Exception e)
            {
                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = $"Write content to blob failed. {e.Message}";
                return healthCheckResult;
            }

            try
            {
                // Ensure we can write to the storage account
                await acrClient.Manifests.GetAsync(imageInfo.ImageName, imageInfo.Label, MediatypeV2Manifest, cancellationToken);
            }
            catch (Exception e)
            {
                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = $"Write content to blob failed. {e.Message}";
                return healthCheckResult;
            }

            return null;
        }

    }
}
