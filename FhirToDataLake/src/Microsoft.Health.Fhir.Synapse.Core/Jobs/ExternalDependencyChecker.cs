﻿// -------------------------------------------------------------------------------------------------
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
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.TemplateManagement.Client;
using Microsoft.Health.Fhir.TemplateManagement.Models;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class ExternalDependencyChecker : IExternalDependencyChecker
    {
        private readonly IAzureBlobContainerClient _blobContainerClient;
        private readonly IFhirDataClient _fhirApiDataClient;

        private readonly string _filterImageReference;
        private readonly string _schemaImageReference;
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;

        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<ExternalDependencyChecker> _logger;

        private const string MediatypeV2Manifest = "application/vnd.docker.distribution.manifest.v2+json";

        public ExternalDependencyChecker(
            IFhirDataClient fhirApiDataClient,
            IAzureBlobContainerClientFactory azureBlobContainerClientFactory,
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            IOptions<FilterLocation> filterLocation,
            IOptions<SchemaConfiguration> schemaConfiguration,
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<DataLakeStoreConfiguration> storeConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<ExternalDependencyChecker> logger)
        {
            _fhirApiDataClient = EnsureArg.IsNotNull(fhirApiDataClient, nameof(fhirApiDataClient));
            EnsureArg.IsNotNull(azureBlobContainerClientFactory, nameof(azureBlobContainerClientFactory));
            _blobContainerClient = azureBlobContainerClientFactory.Create(storeConfiguration.Value.StorageUrl, jobConfiguration.Value.ContainerName);

            _containerRegistryTokenProvider = EnsureArg.IsNotNull(containerRegistryTokenProvider, nameof(containerRegistryTokenProvider));

            EnsureArg.IsNotNull(filterLocation, nameof(filterLocation));
            _filterImageReference = filterLocation.Value.EnableExternalFilter ? filterLocation.Value.FilterImageReference : null;

            EnsureArg.IsNotNull(schemaConfiguration, nameof(schemaConfiguration));
            _schemaImageReference = schemaConfiguration.Value.EnableCustomizedSchema ? schemaConfiguration.Value.SchemaImageReference : null;

            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public async Task<bool> IsExternalDependencyReady(CancellationToken cancellationToken)
        {
            bool isReady = IsBlobContainerReady();

            isReady = isReady && await IsFhirDataClientReady(cancellationToken);

            if (_filterImageReference != null)
            {
                isReady = isReady && await IsAzureContainerRegistryReady(_filterImageReference, cancellationToken);
            }

            if (_schemaImageReference != null)
            {
                isReady = isReady && await IsAzureContainerRegistryReady(_filterImageReference, cancellationToken);
            }

            if (!isReady)
            {
                _logger.LogInformation("The external dependencies are not fully ready.");
                _diagnosticLogger.LogError("The external dependencies are not fully ready.");
            }

            return isReady;
        }

        private bool IsBlobContainerReady()
        {
            bool isReady = _blobContainerClient.IsInitialized();
            if (isReady)
            {
                _logger.LogInformation("Blob storage is initialized.");
                _diagnosticLogger.LogInformation("Blob storage is initialized.");
            }
            else
            {
                _logger.LogInformation("Blob storage is not initialized.");
                _diagnosticLogger.LogError("Blob storage is not initialized.");
            }

            return isReady;
        }

        private async Task<bool> IsFhirDataClientReady(CancellationToken cancellationToken)
        {
            var searchOptions = new BaseSearchOptions("Patient", null);
            try
            {
                // Ensure we can search from FHIR server.
                await _fhirApiDataClient.SearchAsync(searchOptions, cancellationToken);
                _logger.LogInformation("Fhir server is ready.");
                _diagnosticLogger.LogInformation("Fhir server is ready.");
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Fhir server is not accessible.");
                _diagnosticLogger.LogError($"Fhir server is not accessible: {ex.Message}.");
                return false;
            }

            return true;
        }

        private async Task<bool> IsAzureContainerRegistryReady(string imageReference, CancellationToken cancellationToken)
        {
            try
            {
                var imageInfo = ImageInfo.CreateFromImageReference(imageReference);
                string accessToken = await _containerRegistryTokenProvider.GetTokenAsync(imageInfo.Registry, cancellationToken);
                var acrClient = new AzureContainerRegistryClient(imageInfo.Registry, new AcrClientCredentials(accessToken));

                // Ensure we can read from acr.
                await acrClient.Manifests.GetAsync(imageInfo.ImageName, imageInfo.Label, MediatypeV2Manifest, cancellationToken);

                _logger.LogInformation($"ACR {imageReference} is ready.");
                _diagnosticLogger.LogInformation($"ACR {imageReference} is ready.");
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, $"ACR {imageReference} is not accessible.");
                _diagnosticLogger.LogError($"ACR {imageReference} is not accessible: {ex.Message}.");
                return false;
            }

            return true;
        }
    }
}