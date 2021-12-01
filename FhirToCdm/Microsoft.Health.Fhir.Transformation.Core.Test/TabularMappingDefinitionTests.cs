// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Health.Fhir.Transformation.Core.Test
{
    [TestClass]
    public class TabularMappingDefinitionTests
    {
        [TestMethod]
        public void GivenATabularMappingDefinition_WhenGetSchema_AllColumnsShouldBeReturn()
        {
            TabularMappingDefinition defination = new TabularMappingDefinition("Test");
            defination.Root.Children["Test1"] = new DefinitionNode();
            defination.Root.Children["Test1"].Children["Test2"] = new DefinitionNode(new ColumnDefinition("name1", "type1"));
            defination.Root.Children["Test1"].Children["Test3"] = new DefinitionNode(new ColumnDefinition("name2", "type2"));
            defination.Root.Children["Test4"] = new DefinitionNode(new ColumnDefinition("name3", "type3"));

            List<(string, string)> columns = defination.Columns.ToList();
            Assert.IsTrue(columns.Any(c => c.Item1.Contains("name1")));
            Assert.IsTrue(columns.Any(c => c.Item1.Contains("name2")));
            Assert.IsTrue(columns.Any(c => c.Item1.Contains("name3")));
            Assert.IsTrue(columns.Any(c => c.Item1.Contains(ReservedColumnName.ResourceId)));
            Assert.AreEqual(4, columns.Count());
        }

        [TestMethod]
        public void GivenATabularMappingDefinitionWithUnrollPath_WhenGetSchema_AllColumnsShouldBeReturn()
        {
            TabularMappingDefinition defination = new TabularMappingDefinition("Test");
            defination.Unrollpath = "test1";
            defination.Root.Children["Test1"] = new DefinitionNode();
            defination.Root.Children["Test1"].Children["Test2"] = new DefinitionNode(new ColumnDefinition("name1", "type1"));
            defination.Root.Children["Test1"].Children["Test3"] = new DefinitionNode(new ColumnDefinition("name2", "type2"));
            defination.Root.Children["Test4"] = new DefinitionNode(new ColumnDefinition("name3", "type3"));

            List<(string, string)> columns = defination.Columns.ToList();
            Assert.IsTrue(columns.Any(c => c.Item1.Contains("name1")));
            Assert.IsTrue(columns.Any(c => c.Item1.Contains("name2")));
            Assert.IsTrue(columns.Any(c => c.Item1.Contains("name3")));
            Assert.IsTrue(columns.Any(c => c.Item1.Contains(ReservedColumnName.ResourceId)));
            Assert.IsTrue(columns.Any(c => c.Item1.Contains(ReservedColumnName.FhirPath)));
            Assert.IsTrue(columns.Any(c => c.Item1.Contains(ReservedColumnName.ParentPath)));
            Assert.IsTrue(columns.Any(c => c.Item1.Contains(ReservedColumnName.RowId)));
            Assert.AreEqual(7, columns.Count());
        }
    }
}
