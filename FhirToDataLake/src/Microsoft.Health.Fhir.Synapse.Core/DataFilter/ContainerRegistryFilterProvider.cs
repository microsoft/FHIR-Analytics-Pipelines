// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.TemplateManagement.ArtifactProviders;
using Microsoft.Health.Fhir.TemplateManagement.Client;
using Microsoft.Health.Fhir.TemplateManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using Microsoft.Health.Fhir.TemplateManagement.Utilities;
using Newtonsoft.Json;
using Polly;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public class ContainerRegistryFilterProvider : IFilterProvider
    {
        private readonly string _imageReference;
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;
        private readonly ILogger<ContainerRegistryFilterProvider> _logger;
        private const string _configName = "filterConfiguration.json";
        private readonly IDiagnosticLogger _diagnosticLogger;

        public ContainerRegistryFilterProvider(
            IOptions<FilterLocation> filterLocation,
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            IDiagnosticLogger diagnosticLogger,
            ILogger<ContainerRegistryFilterProvider> logger)
        {
            EnsureArg.IsNotNull(filterLocation, nameof(filterLocation));
            _containerRegistryTokenProvider = EnsureArg.IsNotNull(containerRegistryTokenProvider, nameof(containerRegistryTokenProvider));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            _diagnosticLogger = EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));

            _imageReference = filterLocation.Value.FilterImageReference;
        }

        public async Task<FilterConfiguration> GetFilterAsync(CancellationToken cancellationToken)
        {
            ImageInfo imageInfo;
            try
            {
                imageInfo = ImageInfo.CreateFromImageReference(_imageReference);
            }
            catch (ImageReferenceException ex)
            {
                var message = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: Failed to parse filter image reference {0}.", _imageReference);
                _diagnosticLogger.LogError(message);
                _logger.LogInformation(ex, message);
                throw new ContainerRegistryFilterException(message, ex);
            }
            catch (Exception ex)
            {
                var message = string.Format("Failed to fetch filter configuration from image reference {_imageReference}. Reason: Unhandled exception while parsing image reference {_imageReference}.", _imageReference);

                _diagnosticLogger.LogError(message);
                _logger.LogError(ex, message);
                throw;
            }

            var accessToken = await _containerRegistryTokenProvider.GetTokenAsync(imageInfo.Registry, cancellationToken);

            try
            {
                AcrClient client = new AcrClient(imageInfo.Registry, accessToken);
                var filterConfigurationProvider = new OciArtifactProvider(imageInfo, client);
                var acrImage = await Policy
                  .Handle<TemplateManagementException>()
                  .WaitAndRetryAsync(
                    3,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        _logger.LogInformation(exception, "Failed to get image {Image} from Container Registry. Retry {RetryCount}.", _imageReference, retryCount);
                    })
                  .ExecuteAsync(() => filterConfigurationProvider.GetOciArtifactAsync(cancellationToken));

                var blobsSize = acrImage.Blobs.Count;
                for (var i = blobsSize - 1; i >= 0; i--)
                {
                    if (CheckConfigurationCollectionIsTooLarge(acrImage.Blobs[i].Content.LongLength))
                    {
                        var message = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: Configuration collection is too large.", _imageReference);

                        _diagnosticLogger.LogError(message);
                        _logger.LogInformation(message);
                        throw new ContainerRegistryFilterException(message);
                    }

                    using var blobStream = new MemoryStream(acrImage.Blobs[i].Content);
                    var blobsDict = StreamUtility.DecompressFromTarGz(blobStream);
                    if (!blobsDict.Keys.Contains(_configName))
                    {
                        continue;
                    }
                    else
                    {
                        if (CheckConfigurationIsTooLarge(blobsDict[_configName].LongLength))
                        {
                            var message = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: Configuration file is too large.", _imageReference);

                            _diagnosticLogger.LogError(message);
                            _logger.LogInformation(message);
                            throw new ContainerRegistryFilterException(message);
                        }

                        using var config = new MemoryStream(blobsDict[_configName]);
                        config.Position = 0;
                        using (StreamReader reader = new StreamReader(config))
                        {
                            string configurationContent = await reader.ReadToEndAsync();
                            try
                            {
                                return JsonConvert.DeserializeObject<FilterConfiguration>(configurationContent);
                            }
                            catch (Exception ex)
                            {
                                var message = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: Invalid filter format.", _imageReference);

                                _diagnosticLogger.LogError(message);
                                _logger.LogInformation(ex, message);
                                throw new ContainerRegistryFilterException(message, ex);
                            }
                        }
                    }
                }

                var failedMessage = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: {1} not found.", _imageReference, _configName);

                _diagnosticLogger.LogError(failedMessage);
                _logger.LogInformation(failedMessage);
                throw new ContainerRegistryFilterException(failedMessage);
            }
            catch (ContainerRegistryAuthenticationException authEx)
            {
                var message = string.Format("Failed to access container registry: {0}. Authentication failed.", _imageReference);

                _diagnosticLogger.LogError(message);
                _logger.LogInformation(authEx, message);
                throw new ContainerRegistryFilterException(message, authEx);
            }
            catch (TemplateManagementException ex)
            {
                var message = string.Format("Failed to fetch filter configuration from image reference {0}.", _imageReference);

                _diagnosticLogger.LogError(message);
                _logger.LogInformation(ex, message);
                throw new ContainerRegistryFilterException(message, ex);
            }
            catch (ContainerRegistryFilterException)
            {
                throw;
            }
            catch (Exception ex)
            {
                var message = string.Format("Unhandled exception while fetching filter configuration from image reference {0}.", _imageReference);

                _diagnosticLogger.LogError(message);
                _logger.LogError(ex, message);
                throw;
            }
        }

        private static bool CheckConfigurationIsTooLarge(long size) =>
            size > 1 * 1024 * 1024; // Max content length is 1 MB

        private static bool CheckConfigurationCollectionIsTooLarge(long size) =>
            size > 100 * 1024 * 1024; // Max content length is 100 MB
    }
}