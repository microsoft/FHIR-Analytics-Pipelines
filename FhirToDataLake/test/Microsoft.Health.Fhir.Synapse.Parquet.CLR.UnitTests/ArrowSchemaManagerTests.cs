// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
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
            var organizationSchemaNode = new FhirParquetSchemaNode
            {
                ChoiceTypeNodes = new Dictionary<string, Tuple<string, string>>(),
                Depth = 0,
                Name = "Organization",
                NodePaths = new List<string> { "Organization" },
                IsRepeated = false,
                SubNodes = new Dictionary<string, FhirParquetSchemaNode>(),
                Type = "Organization",
            };

            var arrowSchemaManager = new ArrowSchemaManager(TestConstants.TestSchemaMap);
            arrowSchemaManager.AddOrUpdateArrowSchema("Organization", organizationSchemaNode);
        }
    }
}
