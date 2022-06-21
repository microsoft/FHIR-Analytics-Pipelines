// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet
{
    public class FhirParquetSchemaParser
    {
        // Basic types refer to Json schema document https://cswr.github.io/JsonSchema/spec/basic_types/
        private static readonly HashSet<JSchemaType> BasicTypes = new HashSet<JSchemaType>()
        {
            JSchemaType.String,
            JSchemaType.Number,
            JSchemaType.Integer,
            JSchemaType.Boolean,
            JSchemaType.Null,
        };

        public FhirParquetSchemaNode ParseJSchema(string resourceType, JSchema jSchema)
        {
            string schemaType = $@"{resourceType}_Customized";
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

                if (property.Value.Type == null)
                {
                    throw new ParseJsonSchemaException(string.Format("Property \"{0}\" for \"{1}\" customized schema have no \"type\" keyword.", property.Key, resourceType));
                }
            }
        }

        public FhirParquetSchemaNode Build(CustomizedSchemaContent schemaContent)
        {
            string schemaType = $@"{schemaContent.resourceType}_Customized";
            var fhirPath = new List<string>() { schemaType };

            var customizedSchemaNode = new FhirParquetSchemaNode()
            {
                Name = schemaType,
                Type = schemaType,
                Depth = 0,
                NodePaths = new List<string>(fhirPath),
                SubNodes = new Dictionary<string, FhirParquetSchemaNode>(),
            };

            foreach (var column in schemaContent.columns)
            {
                fhirPath.Add(column.columnName);

                if (customizedSchemaNode.SubNodes.ContainsKey(column.columnName))
                {
                    throw new ParseJsonSchemaException($"Found two {column.columnName} for customized schema {schemaType}");
                }

                customizedSchemaNode.SubNodes.Add(
                    column.columnName,
                    BuildLeafNode(column.columnName, column.dataType, 1, fhirPath));

                fhirPath.RemoveAt(fhirPath.Count - 1);
            }

            return customizedSchemaNode;
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
