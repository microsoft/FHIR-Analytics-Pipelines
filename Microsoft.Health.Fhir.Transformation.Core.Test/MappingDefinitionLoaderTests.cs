// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Microsoft.Health.Fhir.Transformation.Core.TabularDefinition;
using Microsoft.Health.Fhir.Transformation.Core.TabularDefinition.Contracts;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Health.Fhir.Transformation.Core.Test
{
    [TestClass]
    public class MappingDefinitionLoaderTests
    {
        [TestMethod]
        public void GivenDefinitionFolder_WhenLoadConfiguration_TabularMappingDefinitionShouldBeReturn()
        {
            BaseMappingDefinitionLoader loader = new LocalMappingDefinitionLoader(".\\TestResource\\TestSchemaSimple");
            var tabularMappingDefinitions = loader.Load().ToList();

            var columns = tabularMappingDefinitions[0].Columns;
            Assert.AreEqual(33, columns.Count());
            Assert.IsTrue(condition: columns.Any(c => c.columnName.Equals(ReservedColumnName.ResourceId)));
            Assert.IsFalse(condition: columns.Any(c => c.columnName.Equals(ReservedColumnName.FhirPath)));
            Assert.IsTrue(condition: columns.Any(c => c.columnName.Equals("NameMulti_seg_path")));
        }

        [TestMethod]
        public void GivenDefinitionFolderWithResourceUnroll_WhenLoadConfiguration_TabularMappingDefinitionShouldBeReturn()
        {
            LocalMappingDefinitionLoader loader = new LocalMappingDefinitionLoader(".\\TestResource\\TestSchemaUnroll");
            var tabularMappingDefinitions = loader.Load().ToList();

            var columns = tabularMappingDefinitions[0].Columns;
            Assert.AreEqual(8, columns.Count());
            Assert.IsTrue(condition: columns.Any(c => c.columnName.Equals(ReservedColumnName.ResourceId)));
            Assert.IsTrue(condition: columns.Any(c => c.columnName.Equals(ReservedColumnName.FhirPath)));
            Assert.IsTrue(condition: columns.Any(c => c.columnName.Equals(ReservedColumnName.RowId)));
            Assert.IsTrue(condition: columns.Any(c => c.columnName.Equals(ReservedColumnName.ParentPath)));
        }

        [TestMethod]
        public void GivenColumnWithShortPath_WhenResolveDefinitionNode_ResolveDefinitionNodesShouldBeReturn()
        {
            Dictionary<string, PropertiesGroup> propertiesGroups = new Dictionary<string, PropertiesGroup>();

            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName",
                Path = "test1"
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, propertiesGroups, 5);
            Assert.AreEqual("TestName", defination.Root.Children["test1"].ColumnDefinitions[0].Name);
        }

        [TestMethod]
        public void GivenColumnWithExpression_WhenResolveDefinitionNode_ResolveDefinitionNodesShouldBeReturn()
        {
            Dictionary<string, PropertiesGroup> propertiesGroups = new Dictionary<string, PropertiesGroup>();

            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName1",
                Path = "",
                FhirExpression = "testExpression1"
            });
            properties.Add(new Property()
            {
                Name = "TestName2",
                Path = "test1",
                FhirExpression = "testExpression2"
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, propertiesGroups, 5);
            Assert.AreEqual("TestName1", defination.Root.ColumnDefinitions[0].Name);
            Assert.AreEqual("testExpression1", defination.Root.ColumnDefinitions[0].FhirExpression);
            Assert.AreEqual("TestName2", defination.Root.Children["test1"].ColumnDefinitions[0].Name);
            Assert.AreEqual("testExpression2", defination.Root.Children["test1"].ColumnDefinitions[0].FhirExpression);
        }

        [TestMethod]
        public void GivenColumnWithMultiExpressionInSamePath_WhenResolveDefinitionNode_ResolveDefinitionNodesShouldBeReturn()
        {
            Dictionary<string, PropertiesGroup> propertiesGroups = new Dictionary<string, PropertiesGroup>();

            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName1",
                Path = "",
                FhirExpression = "testExpression1"
            });
            properties.Add(new Property()
            {
                Name = "TestName2",
                Path = "",
                FhirExpression = "testExpression2"
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, propertiesGroups, 5);
            Assert.AreEqual("TestName1", defination.Root.ColumnDefinitions[0].Name);
            Assert.AreEqual("testExpression1", defination.Root.ColumnDefinitions[0].FhirExpression);
            Assert.AreEqual("TestName2", defination.Root.ColumnDefinitions[1].Name);
            Assert.AreEqual("testExpression2", defination.Root.ColumnDefinitions[1].FhirExpression);
        }

        [TestMethod]
        public void GivenColumnWithLongPath_WhenResolveDefinitionNode_ResolveDefinitionNodesShouldBeReturn()
        {
            Dictionary<string, PropertiesGroup> propertiesGroups = new Dictionary<string, PropertiesGroup>();

            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName",
                Path = "test1.test2.test3.test4.test5"
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, propertiesGroups, 5);
            Assert.AreEqual("Test1Test2Test3Test4TestName", defination.Root.Children["test1"].Children["test2"]
                                                                        .Children["test3"].Children["test4"].Children["test5"].ColumnDefinitions[0].Name);
        }

        [TestMethod]
        public void GivenColumnWith2LongPath_WhenResolveDefinitionNode_MergedResolveDefinitionNodesShouldBeReturn()
        {
            Dictionary<string, PropertiesGroup> propertiesGroups = new Dictionary<string, PropertiesGroup>();

            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName1",
                Path = "test1.test2"
            });
            properties.Add(new Property()
            {
                Name = "TestName2",
                Path = "test1.test3"
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, propertiesGroups, 5);
            Assert.AreEqual("Test1TestName1", defination.Root.Children["test1"].Children["test2"].ColumnDefinitions[0].Name);
            Assert.AreEqual("Test1TestName2", defination.Root.Children["test1"].Children["test3"].ColumnDefinitions[0].Name);
        }

        [TestMethod]
        public void Given2ColumnWithSameName_WhenResolveDefinitionNode_ColumnNameWithPathShouldBeReturn()
        {
            Dictionary<string, PropertiesGroup> propertiesGroups = new Dictionary<string, PropertiesGroup>();

            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName1",
                Path = "test1"
            });
            properties.Add(new Property()
            {
                Name = "TestName1",
                Path = "test1.test2"
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, propertiesGroups, 5);
            Assert.AreEqual("TestName1", defination.Root.Children["test1"].ColumnDefinitions[0].Name);
            Assert.AreEqual("Test1TestName1", defination.Root.Children["test1"].Children["test2"].ColumnDefinitions[0].Name);
        }

        [TestMethod]
        public void GivenColumnWithLongPathGroup_WhenResolveDefinitionNode_ResolveDefinitionNodesShouldBeReturn()
        {
            Dictionary<string, PropertiesGroup> propertiesGroups = new Dictionary<string, PropertiesGroup>();
            propertiesGroups["test"] = new PropertiesGroup()
            {
                PropertiesGroupName = "test",
                Properties = new List<Property>()
                {
                    new Property() { Name = "p1", Type = "string", Path = "abc"}
                }
            };
            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName",
                Path = "test1.test2",
                PropertiesGroup = "test"
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, propertiesGroups, 5);
            Assert.AreEqual("Test1Test2p1", defination.Root.Children["test1"].Children["test2"].Children["abc"].ColumnDefinitions[0].Name);
        }

        [TestMethod]
        public void GivenColumnWithSubLongPathGroup_WhenResolveDefinitionNode_ResolveDefinitionNodesShouldBeReturn()
        {
            Dictionary<string, PropertiesGroup> propertiesGroups = new Dictionary<string, PropertiesGroup>();
            propertiesGroups["test"] = new PropertiesGroup()
            {
                PropertiesGroupName = "test",
                Properties = new List<Property>()
                {
                    new Property() { Name = "p1", Type = "string", Path = "subpath1.subpath2.abc"}
                }
            };
            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName1",
                Path = "test1.test2",
                PropertiesGroup = "test"
            });
            properties.Add(new Property()
            {
                Name = "TestName2",
                Path = "test1.test3",
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, propertiesGroups, 5);
            Assert.AreEqual("Test1TestName2", defination.Root.Children["test1"].Children["test3"].ColumnDefinitions[0].Name);
            Assert.AreEqual("Test1Test2Subpath1Subpath2p1", defination.Root.Children["test1"].Children["test2"]
                                                                      .Children["subpath1"].Children["subpath2"].Children["abc"].ColumnDefinitions[0].Name);
        }

        [TestMethod]
        public void Given2ColumnWithSamePath_WhenResolveDefinitionNode_2ResolveDefinitionNodesShouldBeReturn()
        {
            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName1",
                Path = "test1.test2",
                Type = "string"
            });

            properties.Add(new Property()
            {
                Name = "TestName2",
                Path = "test1.test2",
                Type = "string"
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, new Dictionary<string, PropertiesGroup>(), 5);
            Assert.AreEqual(2, defination.Root.Children["test1"].Children["test2"].ColumnDefinitions.Count());
            Assert.AreEqual("Test1TestName1", defination.Root.Children["test1"].Children["test2"].ColumnDefinitions[0].Name);
            Assert.AreEqual("Test1TestName2", defination.Root.Children["test1"].Children["test2"].ColumnDefinitions[1].Name);
        }

        [TestMethod]
        public void GivenDefinitionMoreThanMaxLevel_WhenResolveDefinitionNode_ResolveDefinitionNodesShouldBeReturn()
        {
            Dictionary<string, PropertiesGroup> propertiesGroups = new Dictionary<string, PropertiesGroup>();
            propertiesGroups["test1"] = new PropertiesGroup()
            {
                PropertiesGroupName = "test1",
                Properties = new List<Property>()
                {
                    new Property() { Name = "p11", Type = "string", Path = "abc1"},
                    new Property() { PropertiesGroup = "test1", Path = "abc2"}
                }
            };

            var properties = new List<Property>();
            properties.Add(new Property()
            {
                Name = "TestName",
                Path = "test1",
                PropertiesGroup = "test1"
            });
            properties.Add(new Property()
            {
                Name = "p1",
                Path = "test",
                Type = "string"
            });
            TabularTable table = new TabularTable();
            table.Properties = properties;

            TabularMappingDefinition defination = BaseMappingDefinitionLoader.BuildMappingDefinition(table, propertiesGroups, 2);
            Assert.AreEqual(3, defination.Columns.Count());
        }
    }
}
