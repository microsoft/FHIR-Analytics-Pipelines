// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Newtonsoft.Json.Schema;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
{
    public class ACRCustomizedSchemaProvider : IParquetSchemaProvider
    {
        private readonly JsonSchemaCollectionProvider _jsonSchemaCollectionsProvider;

        public ACRCustomizedSchemaProvider(
            JsonSchemaCollectionProvider jsonSchemaCollectionProvider)
        {
            _jsonSchemaCollectionsProvider = jsonSchemaCollectionProvider;
        }

        public async Task<Dictionary<string, FhirParquetSchemaNode>> GetSchemasAsync(string schemaImageReference, CancellationToken cancellationToken)
        {
            var jsonSchemaCollection = await _jsonSchemaCollectionsProvider.GetJsonSchemaCollectionAsync(schemaImageReference, cancellationToken);

            return jsonSchemaCollection
                .Select(x => ParseJSchema(GetCustomizedSchemaType(x.Key), x.Value))
                .ToDictionary(x => x.Type, x => x);
        }

        private FhirParquetSchemaNode ParseJSchema(string schemaType, JSchema jSchema)
        {
            if (jSchema.Type == null || jSchema.Type == JSchemaType.Null)
            {
                throw new ParseJsonSchemaException(string.Format("The \"{0}\" customized schema have no \"type\" keyword or \"type\" is null.", schemaType));
            }

            if (jSchema.Type != JSchemaType.Object)
            {
                throw new ParseJsonSchemaException(string.Format("The \"{0}\" customized schema type \"{1}\" should be \"object\".", schemaType, jSchema.Type));
            }

            var fhirPath = new List<string>() { schemaType };

            var customizedSchemaNode = new FhirParquetSchemaNode()
            {
                Name = schemaType,
                Type = schemaType,
                Depth = 0,
                NodePaths = new List<string>(fhirPath),
                SubNodes = new Dictionary<string, FhirParquetSchemaNode>(),
            };

            foreach (var property in jSchema.Properties)
            {
                fhirPath.Add(property.Key);

                if (property.Value.Type == null || property.Value.Type == JSchemaType.Null)
                {
                    throw new ParseJsonSchemaException(string.Format("Property \"{0}\" for \"{1}\" customized schema have no \"type\" keyword or \"type\" is null.", property.Key, schemaType));
                }

                if (!FhirParquetSchemaNodeConstants.BasicJSchemaTypeMap.ContainsKey(property.Value.Type.Value))
                {
                    throw new ParseJsonSchemaException(string.Format("Property \"{0}\" type \"{1}\" for \"{2}\" customized schema is not basic type.", property.Key, property.Value.Type.Value, schemaType));
                }

                customizedSchemaNode.SubNodes.Add(
                    property.Key,
                    BuildLeafNode(property.Key, FhirParquetSchemaNodeConstants.BasicJSchemaTypeMap[property.Value.Type.Value], 1, fhirPath));

                fhirPath.RemoveAt(fhirPath.Count - 1);
            }

            return customizedSchemaNode;
        }

        public string GetCustomizedSchemaType(string resourceType)
        {
            return $"{resourceType}_Customized";
        }

        private FhirParquetSchemaNode BuildLeafNode(string propertyName, string propertyType, int curDepth, List<string> curFhirPath)
        {
            return new FhirParquetSchemaNode()
            {
                Name = propertyName,
                Type = propertyType,
                Depth = curDepth,
                NodePaths = new List<string>(curFhirPath),
            };
        }
    }
}
