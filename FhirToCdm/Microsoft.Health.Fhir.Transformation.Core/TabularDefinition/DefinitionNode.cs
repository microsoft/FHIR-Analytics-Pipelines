// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public class DefinitionNode
    {
        public DefinitionNode() { }

        public DefinitionNode(params ColumnDefinition[] columns)
        {
            ColumnDefinitions.AddRange(columns);
        }

        public List<ColumnDefinition> ColumnDefinitions { get; set; } = new List<ColumnDefinition>();

        public Dictionary<string, DefinitionNode> Children { get; private set; } = new Dictionary<string, DefinitionNode>();
    }
}
