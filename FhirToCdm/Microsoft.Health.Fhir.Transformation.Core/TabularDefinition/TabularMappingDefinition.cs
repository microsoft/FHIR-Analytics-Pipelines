// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public class TabularMappingDefinition
    {
        public TabularMappingDefinition(string tableName)
        {
            TableName = tableName;
        }

        public string TableName { get; private set; }

        public string ResourceType { get; set; }

        public string Unrollpath { get; set; } = null;

        public DefinitionNode Root { get; private set; } = new DefinitionNode();

        public IEnumerable<(string columnName, string type)> Columns
        {
            get
            {
                List<(string columnName, string type)> columns = DiscoverSchema(Root, new List<string>()).OrderBy(t => t.columnName).ToList();
                
                if (!string.IsNullOrEmpty(this.Unrollpath))
                {
                    (string columnName, string type)[] reservedColumns = new (string columnName, string type)[] 
                        {
                            (ReservedColumnName.RowId, FhirTypeNames.String),
                            (ReservedColumnName.ResourceId, FhirTypeNames.String),
                            (ReservedColumnName.FhirPath, FhirTypeNames.String),
                            (ReservedColumnName.ParentPath, FhirTypeNames.String)
                        };

                    columns.InsertRange(0, reservedColumns);
                }
                else
                {
                    columns.Insert(0, (ReservedColumnName.ResourceId, FhirTypeNames.String));
                }

                return columns;
            }
        }

        private IEnumerable<(string columnName, string type)> DiscoverSchema(DefinitionNode root, List<string> columnRecord)
        {
            foreach (ColumnDefinition column in root.ColumnDefinitions)
            {
                if (!string.IsNullOrEmpty(column.Type))
                {
                    yield return (columnName: column.Name, type: column.Type);
                }
            }

            foreach ((string key, DefinitionNode node) in root.Children)
            {
                foreach ((string columnName, string type) in DiscoverSchema(node, columnRecord))
                {
                    yield return (columnName, type);
                }
            }
        }
    }
}
