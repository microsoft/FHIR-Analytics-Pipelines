// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Health.Fhir.Transformation.Core.TabularDefinition.Contracts;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public abstract class BaseMappingDefinitionLoader
    {
        private int _maxDepth;

        public BaseMappingDefinitionLoader(int maxDepth = 3)
        {
            _maxDepth = maxDepth;
        }

        public TabularMappingDefinition[] Load()
        {
            List<TabularMappingDefinition> result = new List<TabularMappingDefinition>();
            Dictionary<string, PropertiesGroup> propertiesGroups = BuildPropertiesGroups(LoadPropertiesGroupsContent());
            IEnumerable<TabularTable> tabularTables = LoadTableDefinitionsContent().Select(content => JsonConvert.DeserializeObject<TabularTable>(content));

            foreach (TabularTable table in tabularTables)
            {
                result.Add(BuildMappingDefinition(table, propertiesGroups, _maxDepth));
            }

            return result.ToArray();
        }

        public abstract IEnumerable<string> LoadPropertiesGroupsContent();

        public abstract IEnumerable<string> LoadTableDefinitionsContent();

        public static Dictionary<string, PropertiesGroup> BuildPropertiesGroups(IEnumerable<string> propertiesGroupsContents)
        {
            Dictionary<string, PropertiesGroup> result = new Dictionary<string, PropertiesGroup>();
            foreach (string groupDefinition in propertiesGroupsContents)
            {
                var propertyGroup = JsonConvert.DeserializeObject<PropertiesGroup>(groupDefinition);
                result[propertyGroup.PropertiesGroupName] = propertyGroup;
            }

            return result;
        }

        public static TabularMappingDefinition BuildMappingDefinition(TabularTable tabularTable, Dictionary<string, PropertiesGroup> propertiesGroups, int maxDepth)
        {
            TabularMappingDefinition result = new TabularMappingDefinition(tabularTable.Name)
            {
                ResourceType = tabularTable.ResourceType,
                Unrollpath = tabularTable.UnrollPath
            };
            
            ResolveDefinitionNode(result.Root, tabularTable.Properties, propertiesGroups, new LinkedList<string>(), new HashSet<string>(), maxDepth);
            
            return result;
        }

        private static void ResolveDefinitionNode(DefinitionNode current, List<Property> properties, Dictionary<string, PropertiesGroup> propertiesGroups, LinkedList<string> paths, HashSet<string> columns, int maxDepth)
        {
            if (properties == null || properties.Count() == 0 || maxDepth <= 0)
            {
                return;
            }
            
            foreach (Property p in properties)
            {
                DefinitionNode pNode = NavigateOrCreateDefinitionNode(current, p.Path);
                var subPaths = SplitPropertyPath(p.Path);
                foreach (var subPath in subPaths)
                {
                    paths.AddLast(subPath);
                }

                if (string.IsNullOrEmpty(p.PropertiesGroup))
                {
                    string camelCasePaths = string.Join("", paths.Where(path => !string.IsNullOrEmpty(path))
                                                                 .SkipLast(1)
                                                                 .Select(path => path.Substring(0, 1).ToUpper() + path.Substring(1)));
                    string columnName = camelCasePaths + p.Name;
                    pNode.ColumnDefinitions.Add(new ColumnDefinition(columnName, p.Type, p.FhirExpression, null));
                    columns.Add(columnName);
                }
                else
                {
                    ResolveDefinitionNode(pNode, propertiesGroups[p.PropertiesGroup].Properties, propertiesGroups, paths, columns, maxDepth - 1);
                }

                foreach (var subPath in subPaths)
                {
                    paths.RemoveLast();
                }
            }
        }

        private static string[] SplitPropertyPath(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                return new string[0];
            }

            return path.Split('.', StringSplitOptions.RemoveEmptyEntries);
        }

        private static DefinitionNode NavigateOrCreateDefinitionNode(DefinitionNode root, string path)
        {
            foreach (string name in SplitPropertyPath(path))
            {
                if (!root.Children.ContainsKey(name))
                {
                    root.Children[name] = new DefinitionNode();
                }

                root = root.Children[name];
            }

            return root;
        }
    }
}
