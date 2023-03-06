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
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Core.ContainerRegistry;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.TemplateManagement.Client;
using Microsoft.Health.Fhir.TemplateManagement.Models;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public class DataLakeExternalDependencyChecker : IExternalDependencyChecker
    {
        private readonly IAzureBlobContainerClient _sourceBlobContainerClient;
        private readonly IAzureBlobContainerClient _blobContainerClient;

        private readonly string _schemaImageReference;
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;

        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<DataLakeExternalDependencyChecker> _logger;

        public DataLakeExternalDependencyChecker(
            IAzureBlobContainerClientFactory azureBlobContainerClientFactory,
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            IOptions<SchemaConfiguration> schemaConfiguration,
            IOptions<DataSourceConfiguration> dataSourceConfiguration,
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<DataLakeStoreConfiguration> storeConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<DataLakeExternalDependencyChecker> logger)
        {
            EnsureArg.IsNotNull(azureBlobContainerClientFactory, nameof(azureBlobContainerClientFactory));
            _sourceBlobContainerClient = azureBlobContainerClientFactory.Create(dataSourceConfiguration.Value.FhirDataLakeStore.StorageUrl, dataSourceConfiguration.Value.FhirDataLakeStore.ContainerName);
            _blobContainerClient = azureBlobContainerClientFactory.Create(storeConfiguration.Value.StorageUrl, jobConfiguration.Value.ContainerName);

            _containerRegistryTokenProvider = EnsureArg.IsNotNull(containerRegistryTokenProvider, nameof(containerRegistryTokenProvider));

            EnsureArg.IsNotNull(schemaConfiguration, nameof(schemaConfiguration));
            _schemaImageReference = schemaConfiguration.Value.EnableCustomizedSchema ? schemaConfiguration.Value.SchemaImageReference : null;

            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public async Task<bool> IsExternalDependencyReady(CancellationToken cancellationToken)
        {
            bool isSourceBlobContainerReady = IsBlobContainerReady(_sourceBlobContainerClient);
            bool isDestinationBlobContainerReady = IsBlobContainerReady(_blobContainerClient);

            bool isSchemaAcrReady = true;
            if (!string.IsNullOrWhiteSpace(_schemaImageReference))
            {
                isSchemaAcrReady = await IsAzureContainerRegistryReady(_schemaImageReference, cancellationToken);
            }

            bool isReady = isSourceBlobContainerReady && isDestinationBlobContainerReady && isSchemaAcrReady;

            if (isReady)
            {
                _logger.LogInformation("[External Dependency Check] The external dependencies are all ready.");
            }
            else
            {
                _logger.LogInformation("[External Dependency Check] The external dependencies are not fully ready.");
                _diagnosticLogger.LogError("The external dependencies are not fully ready.");
            }

            return isReady;
        }

        private bool IsBlobContainerReady(IAzureBlobContainerClient blobContainerClient)
        {
            bool isReady = blobContainerClient.IsInitialized();
            if (isReady)
            {
                _logger.LogInformation("[External Dependency Check] Blob storage is initialized.");
            }
            else
            {
                _logger.LogInformation("[External Dependency Check] Blob storage is not initialized.");
                _diagnosticLogger.LogError("Blob storage is not initialized.");
            }

            return isReady;
        }

        private async Task<bool> IsAzureContainerRegistryReady(string imageReference, CancellationToken cancellationToken)
        {
            try
            {
                var imageInfo = ImageInfo.CreateFromImageReference(imageReference);
                string accessToken = await _containerRegistryTokenProvider.GetTokenAsync(imageInfo.Registry, cancellationToken);
                var acrClient = new AzureContainerRegistryClient(imageInfo.Registry, new AcrClientCredentials(accessToken));

                // Ensure we can read from acr.
                await acrClient.Manifests.GetAsync(imageInfo.ImageName, imageInfo.Label, OciArtifactConstants.AcceptedManifestTypes, cancellationToken);

                _logger.LogInformation($"[External Dependency Check] ACR {imageReference} is ready.");
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"[External Dependency Check] ACR {imageReference} is not accessible: {ex.Message}.");
                _diagnosticLogger.LogError($"ACR {imageReference} is not accessible: {ex.Message}.");
                return false;
            }

            return true;
        }
    }
}