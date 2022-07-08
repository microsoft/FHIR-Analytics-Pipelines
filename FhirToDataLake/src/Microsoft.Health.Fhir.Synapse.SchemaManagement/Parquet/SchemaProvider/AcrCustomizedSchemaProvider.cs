// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Liquid.Converter.Models.Json;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Newtonsoft.Json.Schema;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public class AcrCustomizedSchemaProvider : IParquetSchemaProvider
    {
        private readonly IContainerRegistryTemplateProvider _containerRegistryTemplateProvider;
        private readonly ILogger<AcrCustomizedSchemaProvider> _logger;

        public AcrCustomizedSchemaProvider(
            IContainerRegistryTemplateProvider containerRegistryTemplateProvider,
            ILogger<AcrCustomizedSchemaProvider> logger)
        {
            _containerRegistryTemplateProvider = containerRegistryTemplateProvider;
            _logger = logger;
        }

        public async Task<Dictionary<string, FhirParquetSchemaNode>> GetSchemasAsync(string schemaImageReference, CancellationToken cancellationToken = default)
        {
            var jsonSchemaCollection = await GetJsonSchemaCollectionAsync(cancellationToken);

            return jsonSchemaCollection
                .Select(x => JsonSchemaParser.ParseJSchema(x.Key, x.Value))
                .ToDictionary(x => GetCustomizedSchemaType(x.Type), x => x);
        }

        private async Task<Dictionary<string, JSchema>> GetJsonSchemaCollectionAsync(CancellationToken cancellationToken)
        {
            var result = new Dictionary<string, JSchema>();

            var templateCollections = await _containerRegistryTemplateProvider.GetTemplateCollectionAsync(cancellationToken);

            foreach (var templates in templateCollections)
            {
                foreach (var templateItem in templates)
                {
                    if (templateItem.Key.EndsWith(FhirParquetSchemaConstants.JsonSchemaTemplateFileExtension))
                    {
                        var customizedSchemaKey = Path.ChangeExtension(templateItem.Key, null);
                        customizedSchemaKey = Path.ChangeExtension(customizedSchemaKey, null);

                        if (!(templateItem.Value.Root is JSchemaDocument customizedSchemaDocument) || customizedSchemaDocument.Schema == null)
                        {
                            _logger.LogError($"Invalid Json schema template {templateItem.Key}, no JSchema content be found.");
                            throw new ContainerRegistrySchemaException($"Invalid Json schema template {templateItem.Key}, no JSchema content be found.");
                        }

                        result.Add(customizedSchemaKey, customizedSchemaDocument.Schema);
                    }
                }
            }

            return result;
        }

        private string GetCustomizedSchemaType(string resourceType)
        {
            return $"{resourceType}{FhirParquetSchemaConstants.CustomizedSchemaSuffix}";
        }
    }
}
