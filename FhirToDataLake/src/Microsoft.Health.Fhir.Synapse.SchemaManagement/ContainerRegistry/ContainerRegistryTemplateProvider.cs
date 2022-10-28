// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using DotLiquid;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement;
using Microsoft.Health.Fhir.TemplateManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using Polly;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry
{
    public class ContainerRegistryTemplateProvider : IContainerRegistryTemplateProvider
    {
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;
        private readonly ITemplateCollectionProviderFactory _templateCollectionProviderFactory;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<ContainerRegistryTemplateProvider> _logger;

        public ContainerRegistryTemplateProvider(
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            IDiagnosticLogger diagnosticLogger,
            ILogger<ContainerRegistryTemplateProvider> logger)
        {
            _containerRegistryTokenProvider = containerRegistryTokenProvider;
            _diagnosticLogger = diagnosticLogger;
            _logger = logger;

            var templateCollectionCache = new MemoryCache(new MemoryCacheOptions() { SizeLimit = 100000000 });
            var config = new TemplateCollectionConfiguration();
            _templateCollectionProviderFactory = new TemplateCollectionProviderFactory(templateCollectionCache, Options.Create(config));
        }

        public async Task<List<Dictionary<string, Template>>> GetTemplateCollectionAsync(string schemaImageReference, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(schemaImageReference))
            {
                _diagnosticLogger.LogError("Schema image reference is null or empty.");
                _logger.LogInformation("Schema image reference is null or empty.");
                throw new ContainerRegistrySchemaException("Schema image reference is null or empty.");
            }

            ImageInfo imageInfo;
            try
            {
                imageInfo = ImageInfo.CreateFromImageReference(schemaImageReference);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError(string.Format("Failed to parse the schema image reference {0} to image information. Reason: {1}.", schemaImageReference, ex.Message));
                _logger.LogInformation(ex, string.Format("Failed to parse the schema image reference {0} to image information. Reason: {1}.", schemaImageReference, ex.Message));
                throw new ContainerRegistrySchemaException(string.Format("Failed to parse the schema image reference {0} to image information. Reason: {1}.", schemaImageReference, ex.Message), ex);
            }

            var accessToken = await _containerRegistryTokenProvider.GetTokenAsync(imageInfo.Registry, cancellationToken);

            try
            {
                var provider = _templateCollectionProviderFactory.CreateTemplateCollectionProvider(schemaImageReference, accessToken);

                // "TemplateManagementException" is base exception for "ContainerRegistryAuthenticationException" and "ImageFetchException"
                return await Policy
                  .Handle<TemplateManagementException>()
                  .WaitAndRetryAsync(
                    3,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    onRetry: (exception, timeSpan, retryCount, context) =>
                    {
                        _logger.LogInformation(exception, "Failed to get template collection from {Image}. Retry {RetryCount}.", schemaImageReference, retryCount);
                    })
                  .ExecuteAsync(() => provider.GetTemplateCollectionAsync(cancellationToken));
            }
            catch (ContainerRegistryAuthenticationException authEx)
            {
                _diagnosticLogger.LogError("Failed to access container registry.");
                _logger.LogInformation(authEx, "Failed to access container registry.");
                throw new ContainerRegistrySchemaException("Failed to access container registry.", authEx);
            }
            catch (ImageFetchException fetchEx)
            {
                _diagnosticLogger.LogError($"Failed to fetch template image. Reason: {fetchEx.Message}");
                _logger.LogInformation(fetchEx, $"Failed to fetch template image. Reason: {fetchEx.Message}");
                throw new ContainerRegistrySchemaException("Failed to fetch template image.", fetchEx);
            }
            catch (TemplateManagementException templateEx)
            {
                _diagnosticLogger.LogError($"Template collection is invalid. Reason: {templateEx.Message}");
                _logger.LogInformation(templateEx, $"Template collection is invalid. Reason: {templateEx.Message}");
                throw new ContainerRegistrySchemaException("Template collection is invalid.", templateEx);
            }
            catch (Exception unhandledEx)
            {
                _diagnosticLogger.LogError($"Unknown exception: failed to get template collection. Reason: {unhandledEx.Message}");
                _logger.LogError(unhandledEx, $"Unhandled exception: failed to get template collection. Reason: {unhandledEx.Message}");
                throw;
            }
        }
    }
}
