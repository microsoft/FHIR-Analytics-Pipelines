// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using EnsureThat;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Exceptions;
using NJsonSchema;

namespace Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet.SchemaProvider
{
    public class JsonSchemaParser
    {
        /// <summary>
        /// The input json schema itself "type" should be object.
        /// And the "type" for each property of json schema should be basic types, which means the Json data must be one layer.
        /// </summary>
        /// <param name="resourceType">The resource type.</param>
        /// <param name="jsonSchema">The json schema.</param>
        /// <returns>A FhirParquetSchemaNode instance.</returns>
        public static ParquetSchemaNode ParseJSchema(string resourceType, JsonSchema jsonSchema)
        {
            EnsureArg.IsNotNullOrWhiteSpace(resourceType, nameof(resourceType));
            EnsureArg.IsNotNull(jsonSchema, nameof(jsonSchema));

            if (jsonSchema.Type == JsonObjectType.None || jsonSchema.Type == JsonObjectType.Null)
            {
                throw new GenerateFhirParquetSchemaNodeException(string.Format("The \"{0}\" customized schema have no \"type\" keyword or \"type\" is null.", resourceType));
            }

            if (jsonSchema.Type != JsonObjectType.Object)
            {
                throw new GenerateFhirParquetSchemaNodeException(string.Format("The \"{0}\" customized schema type \"{1}\" should be \"object\".", resourceType, jsonSchema.Type));
            }

            List<string> fhirPath = new List<string>() { resourceType };

            var customizedSchemaNode = new ParquetSchemaNode()
            {
                Name = resourceType,
                Type = resourceType,
                Depth = 0,
                NodePaths = new List<string>(fhirPath),
                SubNodes = new Dictionary<string, ParquetSchemaNode>(),
            };

            foreach (KeyValuePair<string, JsonSchemaProperty> property in jsonSchema.Properties)
            {
                fhirPath.Add(property.Key);

                if (property.Value.Type == JsonObjectType.None || property.Value.Type == JsonObjectType.Null)
                {
                    throw new GenerateFhirParquetSchemaNodeException(string.Format("Property \"{0}\" for \"{1}\" customized schema have no \"type\" keyword or \"type\" is null.", property.Key, resourceType));
                }

                if (!ParquetSchemaConstants.BasicJSchemaTypeMap.ContainsKey(property.Value.Type))
                {
                    throw new GenerateFhirParquetSchemaNodeException(string.Format("Property \"{0}\" type \"{1}\" for \"{2}\" customized schema is not basic type.", property.Key, property.Value.Type, resourceType));
                }

                customizedSchemaNode.SubNodes.Add(
                    property.Key,
                    BuildLeafNode(property.Key, ParquetSchemaConstants.BasicJSchemaTypeMap[property.Value.Type], 1, fhirPath));

                fhirPath.RemoveAt(fhirPath.Count - 1);
            }

            return customizedSchemaNode;
        }

        private static ParquetSchemaNode BuildLeafNode(string propertyName, string propertyType, int curDepth, List<string> curFhirPath)
        {
            return new ParquetSchemaNode()
            {
                Name = propertyName,
                Type = propertyType,
                Depth = curDepth,
                IsLeaf = true,
                NodePaths = new List<string>(curFhirPath),
            };
        }
    }
}
