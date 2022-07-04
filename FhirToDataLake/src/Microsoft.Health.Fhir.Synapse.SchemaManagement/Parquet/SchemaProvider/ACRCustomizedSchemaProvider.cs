// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotLiquid;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement;
using Microsoft.Health.Fhir.TemplateManagement.Exceptions;
using Microsoft.Health.Fhir.TemplateManagement.Models;
using Newtonsoft.Json.Schema;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public class ACRCustomizedSchemaProvider : IParquetSchemaProvider
    {
        private readonly IContainerRegistryTokenProvider _containerRegistryTokenProvider;
        private readonly ITemplateCollectionProviderFactory _templateCollectionProviderFactory;
        private readonly ILogger<ACRCustomizedSchemaProvider> _logger;

        public ACRCustomizedSchemaProvider(
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            ILogger<ACRCustomizedSchemaProvider> logger)
        {
            _containerRegistryTokenProvider = containerRegistryTokenProvider;
            _logger = logger;

            var templateCollectionCache = new MemoryCache(new MemoryCacheOptions() { SizeLimit = 100000000 });
            var config = new TemplateCollectionConfiguration();
            _templateCollectionProviderFactory = new TemplateCollectionProviderFactory(templateCollectionCache, Options.Create(config));
        }

        public async Task<Dictionary<string, FhirParquetSchemaNode>> GetSchemasAsync(string schemaImageReference, CancellationToken cancellationToken = default)
        {
            var jsonSchemaCollection = await GetJsonSchemaCollectionAsync(schemaImageReference, cancellationToken);

            return jsonSchemaCollection
                .Select(x => JsonSchemaParser.ParseJSchema(GetCustomizedSchemaType(x.Key), x.Value))
                .ToDictionary(x => x.Type, x => x);
        }

        private string GetCustomizedSchemaType(string resourceType)
        {
            return $"{resourceType}_Customized";
        }

        private async Task<Dictionary<string, JSchema>> GetJsonSchemaCollectionAsync(string schemaImageReference, CancellationToken cancellationToken)
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
                _logger.LogError(authEx, "Failed to access container registry.");
                throw new ContainerRegistrySchemaException("Failed to access container registry.", authEx);
            }
            catch (ImageFetchException fetchEx)
            {
                _logger.LogError(fetchEx, "Failed to fetch template image.");
                throw new ContainerRegistrySchemaException("Failed to fetch template image.", fetchEx);
            }
            catch (TemplateManagementException templateEx)
            {
                _logger.LogError(templateEx, "Template collection is invalid.");
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
