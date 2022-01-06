// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;

namespace Microsoft.Health.Fhir.Synapse.Parquet.CLR.UnitTests
{
    public static class TestConstants
    {
        public static Dictionary<string, FhirParquetSchemaNode> TestSchemaMap = CreateSchemaMap();

        private static Dictionary<string, FhirParquetSchemaNode> CreateSchemaMap()
        {
            var schemaMap = new Dictionary<string, FhirParquetSchemaNode>();

            // Mock Patient schema node
            var idSchemaNode = new FhirParquetSchemaNode
            {
                ChoiceTypeNodes = null,
                Depth = 1,
                Name = "id",
                NodePaths = new List<string> { "Patient", "id" },
                IsRepeated = false,
                SubNodes = null,
                Type = "id",
            };

            schemaMap["Patient"] = new FhirParquetSchemaNode
            {
                ChoiceTypeNodes = new Dictionary<string, Tuple<string, string>>
                {
                    { "deceasedBoolean", new Tuple<string, string>("deceased", "boolean") },
                    { "deceasedDateTime", new Tuple<string, string>("deceased", "dateTime") },
                },
                Depth = 0,
                Name = "Patient",
                IsRepeated = false,
                SubNodes = new Dictionary<string, FhirParquetSchemaNode>
                {
                    { "id", idSchemaNode },
                },
                Type = "Patient",
            };

            // Mock Observation schema node
            idSchemaNode.NodePaths[0] = "Observation";
            schemaMap["Observation"] = new FhirParquetSchemaNode
            {
                ChoiceTypeNodes = new Dictionary<string, Tuple<string, string>>
                {
                    { "effectiveDateTime", new Tuple<string, string>("effective", "dateTime") },
                    { "effectivePeriod", new Tuple<string, string>("effective", "period") },
                },
                Depth = 0,
                Name = "Observation",
                NodePaths = new List<string> { "Observation" },
                IsRepeated = false,
                SubNodes = new Dictionary<string, FhirParquetSchemaNode>
                {
                    { "id", idSchemaNode },
                },
                Type = "Observation",
            };

            return schemaMap;
        }
    }
}
