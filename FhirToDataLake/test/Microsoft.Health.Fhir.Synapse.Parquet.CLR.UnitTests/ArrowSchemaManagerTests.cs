using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Parquet.CLR.UnitTests
{
    public class ArrowSchemaManagerTests
    {
        [Fact]
        public void GivenNoSchemaMapOrASchemaMap_WhenCreateArrowSchemaManager_ArrowSchemaManagerShouldBeReturned()
        {
            _ = new ArrowSchemaManager();
            _ = new ArrowSchemaManager(TestConstants.TestSchemaMap);
        }

        [Fact]
        public void GivenArrowSchemaManager_WhenAddSchema_SchemaShouldBeAddedWithoutException()
        {
            // Mock Organization schema node
            var organizationSchemaNode = new FhirSchemaNode
            {
                ChoiceTypeNodes = new Dictionary<string, Tuple<string, string>>(),
                Depth = 0,
                Name = "Organization",
                NodePaths = new List<string> { "Organization" },
                IsRepeated = false,
                SubNodes = new Dictionary<string, FhirSchemaNode>(),
                Type = "Organization",
            };

            var arrowSchemaManager = new ArrowSchemaManager(TestConstants.TestSchemaMap);
            arrowSchemaManager.AddOrUpdateArrowSchema("Organization", organizationSchemaNode);
        }
    }
}
