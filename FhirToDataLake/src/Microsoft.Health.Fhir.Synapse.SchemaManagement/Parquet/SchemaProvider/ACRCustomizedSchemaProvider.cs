// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Newtonsoft.Json.Schema;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public class ACRCustomizedSchemaProvider : IParquetSchemaProvider
    {
        private readonly IContainerRegistryTemplateProvider _containerRegistryTemplateProvider;
        private readonly ILogger<ACRCustomizedSchemaProvider> _logger;

        public ACRCustomizedSchemaProvider(
            IContainerRegistryTemplateProvider containerRegistryTemplateProvider,
            ILogger<ACRCustomizedSchemaProvider> logger)
        {
            _containerRegistryTemplateProvider = containerRegistryTemplateProvider;
            _logger = logger;
        }

        public async Task<Dictionary<string, FhirParquetSchemaNode>> GetSchemasAsync(string schemaImageReference, CancellationToken cancellationToken = default)
        {
            var jsonSchemaCollection = await GetJsonSchemaCollectionAsync(schemaImageReference, cancellationToken);

            return jsonSchemaCollection
                .Select(x => JsonSchemaParser.ParseJSchema(GetCustomizedSchemaType(x.Key), x.Value))
                .ToDictionary(x => x.Type, x => x);
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

        private string GetCustomizedSchemaType(string resourceType)
        {
            return $"{resourceType}_Customized";
        }
    }
}
