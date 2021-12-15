using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;

namespace Microsoft.Health.Fhir.Synapse.Parquet.CLR.UnitTests
{
    public static class TestConstants
    {
        public static Dictionary<string, FhirSchemaNode> TestSchemaMap = CreateSchemaMap();

        private static Dictionary<string, FhirSchemaNode> CreateSchemaMap()
        {
            var schemaMap = new Dictionary<string, FhirSchemaNode>();

            // Mock Patient schema node
            var idSchemaNode = new FhirSchemaNode
            {
                ChoiceTypeNodes = null,
                Depth = 1,
                Name = "id",
                NodePaths = new List<string> { "Patient", "id" },
                IsRepeated = false,
                SubNodes = null,
                Type = "id",
            };

            schemaMap["Patient"] = new FhirSchemaNode
            {
                ChoiceTypeNodes = new Dictionary<string, Tuple<string, string>>
                {
                    { "deceasedBoolean", new Tuple<string, string>("deceased", "boolean") },
                    { "deceasedDateTime", new Tuple<string, string>("deceased", "dateTime") },
                },
                Depth = 0,
                Name = "Patient",
                IsRepeated = false,
                SubNodes = new Dictionary<string, FhirSchemaNode>
                {
                    { "id", idSchemaNode },
                },
                Type = "Patient",
            };

            // Mock Observation schema node
            idSchemaNode.NodePaths[0] = "Observation";
            schemaMap["Observation"] = new FhirSchemaNode
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
                SubNodes = new Dictionary<string, FhirSchemaNode>
                {
                    { "id", idSchemaNode },
                },
                Type = "Observation",
            };

            return schemaMap;
        }
    }
}
