// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Newtonsoft.Json.Schema;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider
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
        public static FhirParquetSchemaNode ParseJSchema(string resourceType, JSchema jsonSchema)
        {
            if (jsonSchema.Type == null || jsonSchema.Type == JSchemaType.Null)
            {
                throw new GenerateFhirParquetSchemaNodeException(string.Format("The \"{0}\" customized schema have no \"type\" keyword or \"type\" is null.", resourceType));
            }

            if (jsonSchema.Type != JSchemaType.Object)
            {
                throw new GenerateFhirParquetSchemaNodeException(string.Format("The \"{0}\" customized schema type \"{1}\" should be \"object\".", resourceType, jsonSchema.Type));
            }

            var fhirPath = new List<string>() { resourceType };

            var customizedSchemaNode = new FhirParquetSchemaNode()
            {
                Name = resourceType,
                Type = resourceType,
                Depth = 0,
                NodePaths = new List<string>(fhirPath),
                SubNodes = new Dictionary<string, FhirParquetSchemaNode>(),
            };

            foreach (var property in jsonSchema.Properties)
            {
                fhirPath.Add(property.Key);

                if (property.Value.Type == null || property.Value.Type == JSchemaType.Null)
                {
                    throw new GenerateFhirParquetSchemaNodeException(string.Format("Property \"{0}\" for \"{1}\" customized schema have no \"type\" keyword or \"type\" is null.", property.Key, resourceType));
                }

                if (!FhirParquetSchemaConstants.BasicJSchemaTypeMap.ContainsKey(property.Value.Type.Value))
                {
                    throw new GenerateFhirParquetSchemaNodeException(string.Format("Property \"{0}\" type \"{1}\" for \"{2}\" customized schema is not basic type.", property.Key, property.Value.Type.Value, resourceType));
                }

                customizedSchemaNode.SubNodes.Add(
                    property.Key,
                    BuildLeafNode(property.Key, FhirParquetSchemaConstants.BasicJSchemaTypeMap[property.Value.Type.Value], 1, fhirPath));

                fhirPath.RemoveAt(fhirPath.Count - 1);
            }

            return customizedSchemaNode;
        }

        private static FhirParquetSchemaNode BuildLeafNode(string propertyName, string propertyType, int curDepth, List<string> curFhirPath)
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
