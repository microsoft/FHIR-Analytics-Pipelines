// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Azure.ContainerRegistry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Core.ContainerRegistry;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Dicom;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Fhir;
using Microsoft.Health.AnalyticsConnector.DataClient.Models;
using Microsoft.Health.AnalyticsConnector.DataClient.Models.DicomApiOption;
using Microsoft.Health.AnalyticsConnector.DataClient.Models.FhirApiOption;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.TemplateManagement.Client;
using Microsoft.Health.Fhir.TemplateManagement.Models;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public class ExternalDependencyChecker : IExternalDependencyChecker
    {
        private readonly IAzureBlobContainerClient _blobContainerClient;
        private readonly DataSourceType _dataSourceType;
        private readonly IApiDataClient _apiDataClient;

        private readonly string _filterImageReference;
        private readonly string _schemaImageReference;
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;

        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<ExternalDependencyChecker> _logger;

        public ExternalDependencyChecker(
            IApiDataClient apiDataClient,
            IAzureBlobContainerClientFactory azureBlobContainerClientFactory,
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            IOptions<FilterLocation> filterLocation,
            IOptions<SchemaConfiguration> schemaConfiguration,
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<DataLakeStoreConfiguration> storeConfiguration,
            IOptions<DataSourceConfiguration> dataSourceConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<ExternalDependencyChecker> logger)
        {
            _apiDataClient = EnsureArg.IsNotNull(apiDataClient, nameof(apiDataClient));
            EnsureArg.IsNotNull(azureBlobContainerClientFactory, nameof(azureBlobContainerClientFactory));
            _blobContainerClient = azureBlobContainerClientFactory.Create(storeConfiguration.Value.StorageUrl, jobConfiguration.Value.ContainerName);

            _containerRegistryTokenProvider = EnsureArg.IsNotNull(containerRegistryTokenProvider, nameof(containerRegistryTokenProvider));

            EnsureArg.IsNotNull(filterLocation, nameof(filterLocation));
            _filterImageReference = filterLocation.Value.EnableExternalFilter ? filterLocation.Value.FilterImageReference : null;

            EnsureArg.IsNotNull(schemaConfiguration, nameof(schemaConfiguration));
            _schemaImageReference = schemaConfiguration.Value.EnableCustomizedSchema ? schemaConfiguration.Value.SchemaImageReference : null;

            EnsureArg.IsNotNull(dataSourceConfiguration, nameof(dataSourceConfiguration));
            _dataSourceType = dataSourceConfiguration.Value.Type;

            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public async Task<bool> IsExternalDependencyReady(CancellationToken cancellationToken)
        {
            bool isBlobReady = IsBlobContainerReady();

            bool isApiServerReady = await IsApiDataClientReady(cancellationToken);

            bool isFilterAcrReady = true;
            if (!string.IsNullOrWhiteSpace(_filterImageReference))
            {
                isFilterAcrReady = await IsAzureContainerRegistryReady(_filterImageReference, cancellationToken);
            }

            bool isSchemaAcrReady = true;
            if (!string.IsNullOrWhiteSpace(_schemaImageReference))
            {
                isSchemaAcrReady = await IsAzureContainerRegistryReady(_schemaImageReference, cancellationToken);
            }

            bool isReady = isBlobReady && isApiServerReady && isFilterAcrReady && isSchemaAcrReady;

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

        private bool IsBlobContainerReady()
        {
            bool isReady = _blobContainerClient.IsInitialized();
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

        private async Task<bool> IsApiDataClientReady(CancellationToken cancellationToken)
        {
            BaseApiOptions searchOptions;
            switch (_dataSourceType)
            {
                case DataSourceType.FHIR:
                    var fhirQueryParameters = new List<KeyValuePair<string, string>>
                    {
                        new KeyValuePair<string, string>(FhirApiConstants.PageCountKey, FhirApiPageCount.Single.ToString("d")),
                    };
                    searchOptions = new BaseSearchOptions("Patient", fhirQueryParameters);
                    break;
                case DataSourceType.DICOM:
                    var dicomQueryParameters = new List<KeyValuePair<string, string>>
                    {
                        new KeyValuePair<string, string>(DicomApiConstants.IncludeMetadataKey, "false"),
                    };
                    searchOptions = new ChangeFeedLatestOptions(dicomQueryParameters);
                    break;
                default:
                    // Should not happen.
                    _logger.LogInformation($"[External Dependency Check] Unsupported data source type: {_dataSourceType}");
                    _diagnosticLogger.LogError($"Unsupported data source type: {_dataSourceType}");
                    return false;
            }

            try
            {
                // Ensure we can search from the source API server.
                await _apiDataClient.SearchAsync(searchOptions, cancellationToken);
                _logger.LogInformation("[External Dependency Check] The source API server is ready.");
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"[External Dependency Check] The source API server is not accessible: {ex.Message}.");
                _diagnosticLogger.LogError($"The source API server is not accessible: {ex.Message}.");
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