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

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public class ContainerRegistryFilterProvider : IFilterProvider
    {
        private readonly string _imageReference;
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;
        private readonly ILogger<ContainerRegistryFilterProvider> _logger;
        private readonly string _configName;
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
            _configName = filterLocation.Value.FilterConfigurationFileName;
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
                var reason = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: Failed to parse filter image reference {0}.", _imageReference);
                _diagnosticLogger.LogError(reason);
                _logger.LogInformation(ex, reason);
                throw new ContainerRegistryFilterException(reason, ex);
            }
            catch (Exception ex)
            {
                var reason = string.Format("Failed to fetch filter configuration from image reference {_imageReference}. Reason: Unhandeled exception while parsing image reference {_imageReference}.", _imageReference);

                _diagnosticLogger.LogError(reason);
                _logger.LogError(ex, reason);
                throw;
            }

            var accessToken = await _containerRegistryTokenProvider.GetTokenAsync(imageInfo.Registry, cancellationToken);

            try
            {
                AcrClient client = new AcrClient(imageInfo.Registry, accessToken);
                var filterConfigurationProvider = new OciArtifactProvider(imageInfo, client);
                var acrImage = await filterConfigurationProvider.GetOciArtifactAsync(cancellationToken);

                var blobsSize = acrImage.Blobs.Count;
                for (var i = blobsSize - 1; i >= 0; i--)
                {
                    if (CheckConfigurationCollectionIsTooLarge(acrImage.Blobs[i].Content.LongLength))
                    {
                        var reason = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: Configuration collection is too large.", _imageReference);

                        _diagnosticLogger.LogError(reason);
                        _logger.LogInformation(reason);
                        throw new ContainerRegistryFilterException(reason);
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
                            var reason = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: Configuration file is too large.", _imageReference);

                            _diagnosticLogger.LogError(reason);
                            _logger.LogInformation(reason);
                            throw new ContainerRegistryFilterException(reason);
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
                                var reason = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: Invalid filter format.", _imageReference);

                                _diagnosticLogger.LogError(reason);
                                _logger.LogInformation(ex, reason);
                                throw new ContainerRegistryFilterException(reason, ex);
                            }
                        }
                    }
                }

                var failReason = string.Format("Failed to fetch filter configuration from image reference {0}. Reason: {1} not found.", _imageReference, _configName);

                _diagnosticLogger.LogError(failReason);
                _logger.LogInformation(failReason);
                throw new ContainerRegistryFilterException(failReason);
            }
            catch (ContainerRegistryAuthenticationException authEx)
            {
                var reason = string.Format("Failed to access container registry: {0}. Authentication failed.", _imageReference);

                _diagnosticLogger.LogError(reason);
                _logger.LogInformation(authEx, reason);
                throw new ContainerRegistryFilterException(reason, authEx);
            }
            catch (TemplateManagementException ex)
            {
                var reason = string.Format("Failed to fetch filter configuration from image reference {0}.", _imageReference);

                _diagnosticLogger.LogError(reason);
                _logger.LogInformation(ex, reason);
                throw new ContainerRegistryFilterException(reason, ex);
            }
            catch (ContainerRegistryFilterException)
            {
                throw;
            }
            catch (Exception ex)
            {
                var reason = string.Format("Unhandled exception while fetching filter configuration from image reference {0}.", _imageReference);

                _diagnosticLogger.LogError(reason);
                _logger.LogError(ex, reason);
                throw;
            }
        }

        private static bool CheckConfigurationIsTooLarge(long size) =>
            size > 1 * 1024 * 1024; // Max content length is 1 MB

        private static bool CheckConfigurationCollectionIsTooLarge(long size) =>
            size > 100 * 1024 * 1024; // Max content length is 100 MB
    }
}