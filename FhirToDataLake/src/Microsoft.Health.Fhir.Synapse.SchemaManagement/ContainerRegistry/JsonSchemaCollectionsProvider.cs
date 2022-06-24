// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DotLiquid;
using EnsureThat;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement;
using Microsoft.Health.Fhir.TemplateManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using Newtonsoft.Json.Schema;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry
{
    public class JsonSchemaCollectionProvider
    {
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;
        private readonly ITemplateCollectionProviderFactory _templateCollectionProviderFactory;
        private readonly ILogger<JsonSchemaCollectionProvider> _logger;

        public JsonSchemaCollectionProvider(
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            ILogger<JsonSchemaCollectionProvider> logger)
        {
            EnsureArg.IsNotNull(containerRegistryTokenProvider, nameof(containerRegistryTokenProvider));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _logger = logger;

            _containerRegistryTokenProvider = containerRegistryTokenProvider;

            var templateCollectionCache = new MemoryCache(new MemoryCacheOptions() { SizeLimit = 100000000 });
            var config = new TemplateCollectionConfiguration();
            _templateCollectionProviderFactory = new TemplateCollectionProviderFactory(templateCollectionCache, Options.Create(config));
        }

        /// <summary>
        /// Fetch Json schema collection from container registry or built-in archive.
        /// </summary>
        /// <param name="schemaImageReference">The Json schema template reference.</param>
        /// <param name="cancellationToken">Cancellation token to cancel the fetch operation.</param>
        /// <returns>Json schema collection.</returns>
        public async Task<Dictionary<string, JSchema>> GetJsonSchemaCollectionAsync(string schemaImageReference, CancellationToken cancellationToken)
        {
            /*
            List<Dictionary<string, Template>> templateCollections = await GetTemplateCollectionsAsync(schemaImageReference, cancellationToken);
            */

            // Will implement later when integrating the FHIR-Converter.
            // Will do a try drive among templates for all resource types with empty Json data, to get involved Json schemas.
            throw new NotImplementedException();
        }

        private async Task<List<Dictionary<string, Template>>> GetTemplateCollectionsAsync(string schemaImageReference, CancellationToken cancellationToken)
        {
            ImageInfo imageInfo = ImageInfo.CreateFromImageReference(schemaImageReference);
            var accessToken = await _containerRegistryTokenProvider.GetTokenAsync(imageInfo.Registry, cancellationToken);

            try
            {
                var provider = _templateCollectionProviderFactory.CreateTemplateCollectionProvider(schemaImageReference, accessToken);
                return await provider.GetTemplateCollectionAsync(cancellationToken);
            }
            catch (ContainerRegistryAuthenticationException authEx)
            {
                _logger.LogWarning(authEx, "Failed to access container registry.");
                throw new ContainerRegistrySchemaException("Failed to access container registry.", authEx);
            }
            catch (ImageFetchException fetchEx)
            {
                _logger.LogWarning(fetchEx, "Failed to fetch template image.");
                throw new ContainerRegistrySchemaException("Failed to fetch template image.", fetchEx);
            }
            catch (TemplateManagementException templateEx)
            {
                _logger.LogWarning(templateEx, "Template collection is invalid.");
                throw new ContainerRegistrySchemaException("Template collection is invalid.", templateEx);
            }
            catch (Exception unhandledEx)
            {
                _logger.LogError(unhandledEx, "Unhandled exception: failed to get template collection.");
                throw new ContainerRegistrySchemaException("Unhandled exception: failed to get template collection.", unhandledEx);
            }
        }
    }
}
