// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Health.Fhir.Transformation.Core.Test
{
    [TestClass]
    public class BasicFhirElementTabularTransformerTests
    {
        [TestMethod]
        public void GivenAFhirResourceAndColumnDefinition_WhenToTabularSource_ValuesAndTypesShouldBeReturn()
        {
            BasicFhirElementTabularTransformer t = new BasicFhirElementTabularTransformer();
            Patient patient = new Patient() { Gender = AdministrativeGender.Male };
            patient.Address.Add(new Address() { City = "Shanghai" });

            ElementNode node = ElementNode.FromElement(patient.ToTypedElement());
            TabularMappingDefinition defination = new TabularMappingDefinition("Test");
            defination.Root.Children["address"] = new DefinitionNode(new ColumnDefinition("column1", "string"));
            defination.Root.Children["address"].Children["city"] = new DefinitionNode(new ColumnDefinition("column2", "string"));
            Dictionary<string, (object, object)> result = t.ToTabular(node, defination);
            
            Assert.AreEqual("{\"city\":\"Shanghai\"}", result["column1"].Item1);
            Assert.AreEqual("string", result["column1"].Item2);
            Assert.AreEqual("Shanghai", result["column2"].Item1);
            Assert.AreEqual("string", result["column2"].Item2);
        }

        [TestMethod]
        public void Given2ColumnDefinitionOfSamePath_WhenToTabularSource_BothValuesAndTypesShouldBeReturn()
        {
            BasicFhirElementTabularTransformer t = new BasicFhirElementTabularTransformer();
            Patient patient = new Patient() { Gender = AdministrativeGender.Male };
            patient.Address.Add(new Address() { City = "Shanghai" });

            ElementNode node = ElementNode.FromElement(patient.ToTypedElement());
            TabularMappingDefinition defination = new TabularMappingDefinition("Test");
            defination.Root.Children["address"] = new DefinitionNode(new ColumnDefinition("column1", "string"), new ColumnDefinition("column2", "string"));
            Dictionary<string, (object, object)> result = t.ToTabular(node, defination);

            Assert.AreEqual("{\"city\":\"Shanghai\"}", result["column1"].Item1);
            Assert.AreEqual("{\"city\":\"Shanghai\"}", result["column2"].Item1);
        }

        [TestMethod]
        public void GivenConfigurationWithInvalidPath_WhenToTabularSource_NullValueShouldBeReturn()
        {
            BasicFhirElementTabularTransformer t = new BasicFhirElementTabularTransformer();
            Patient patient = new Patient() { Gender = AdministrativeGender.Male };
            patient.Address.Add(new Address() { City = "Shanghai" });
            ElementNode node = ElementNode.FromElement(patient.ToTypedElement());
            
            TabularMappingDefinition defination = new TabularMappingDefinition("Test");
            defination.Root.Children["invalid"] = new DefinitionNode();
            defination.Root.Children["invalid"].Children["invalid"] = new DefinitionNode(new ColumnDefinition("column1", "string"));
            Dictionary<string, (object, object)> result = t.ToTabular(node, defination);

            Assert.IsNull(result["column1"].Item1);
            Assert.AreEqual("string", result["column1"].Item2);
        }

        [TestMethod]
        public void GivenColumnDefinitionWithDifferentTypes_WhenToTabularSource_ValueCanBeConvertedShouldBeReturn()
        {
            BasicFhirElementTabularTransformer t = new BasicFhirElementTabularTransformer();
            Patient patient = new Patient() { Gender = AdministrativeGender.Male };
            patient.Address.Add(new Address() { City = "Shanghai" });
            patient.BirthDate = "1974-12-25";
            patient.Telecom.Add(new ContactPoint() { Rank = 1 });
            ElementNode node = ElementNode.FromElement(patient.ToTypedElement());
            
            TabularMappingDefinition defination = new TabularMappingDefinition("Test");
            defination.Root.Children["address"] = new DefinitionNode();
            defination.Root.Children["address"].Children["city"] = new DefinitionNode(new ColumnDefinition("column1", "string"));
            defination.Root.Children["birthDate"] = new DefinitionNode(new ColumnDefinition("column2", "date"));
            defination.Root.Children["telecom"] = new DefinitionNode();
            defination.Root.Children["telecom"].Children["rank"] = new DefinitionNode(new ColumnDefinition("column3", "integer"));
            Dictionary<string, (object, object)> result = t.ToTabular(node, defination);

            Assert.AreEqual("Shanghai", result["column1"].Item1);
            Assert.AreEqual("string", result["column1"].Item2);
            
            Assert.AreEqual(DateTime.Parse("1974-12-25"), result["column2"].Item1);
            Assert.AreEqual("date", result["column2"].Item2);

            Assert.AreEqual(1, result["column3"].Item1);
            Assert.AreEqual("integer", result["column3"].Item2);
        }

        [TestMethod]
        public void GivenUnrollPathConfiguration_WhenToTabularSource_MultipleItemsShouldBeReturn()
        {
            BasicFhirElementTabularTransformer t = new BasicFhirElementTabularTransformer();
            Patient patient = new Patient() { Gender = AdministrativeGender.Male };
            patient.Address.Add(new Address() { City = "Shanghai" });
            patient.Address.Add(new Address() { City = "Beijing" });
            patient.BirthDate = "1974-12-25";
            patient.Id = "Test1";
            patient.Telecom.Add(new ContactPoint() { Rank = 1 });

            TabularMappingDefinition defination = new TabularMappingDefinition("Test");
            defination.Unrollpath = "Patient.address";
            defination.Root.Children["city"] = new DefinitionNode(new ColumnDefinition("column1", "string"));
            IEnumerable<Dictionary<string, (object, object)>> results = t.ToTabular(patient, defination);

            Assert.AreEqual(2, results.Count());

            foreach (var result in results)
            {
                Assert.AreEqual("Test1", result[ReservedColumnName.ResourceId].Item1);
                Assert.IsTrue(result.ContainsKey(ReservedColumnName.FhirPath));
                Assert.IsTrue(result.ContainsKey("column1"));
                Assert.IsTrue(!string.IsNullOrEmpty(result["column1"].Item1?.ToString()));
            }
        }

        [TestMethod]
        public void GivenColumnDefinitionWithFhirExpressionInRoot_WhenToTabularSource_EvaluatedValueShourlBeReturn()
        {
            BasicFhirElementTabularTransformer t = new BasicFhirElementTabularTransformer();
            Patient patient = new Patient() { Gender = AdministrativeGender.Male };
            patient.Id = "TestId";
            patient.BirthDate = "1974-12-25";

            ElementNode node = ElementNode.FromElement(patient.ToTypedElement());
            TabularMappingDefinition defination = new TabularMappingDefinition("Test");
            defination.Root.Children[""] = new DefinitionNode(new ColumnDefinition("column1", "string", "id", null), 
                                                              new ColumnDefinition("column2", "string", "birthDate", null));
            Dictionary<string, (object, object)> result = t.ToTabular(node, defination);

            Assert.AreEqual("TestId", result["column1"].Item1);
            Assert.AreEqual("1974-12-25", result["column2"].Item1);
        }

        [TestMethod]
        public void GivenColumnDefinitionWithFhirExpressionInChildren_WhenToTabularSource_EvaluatedValueShourlBeReturn()
        {
            BasicFhirElementTabularTransformer t = new BasicFhirElementTabularTransformer();
            Patient patient = new Patient() { Gender = AdministrativeGender.Male };
            patient.Address.Add(new Address() { City = "Shanghai" });
            patient.Address.Add(new Address() { City = "Beijing" });

            ElementNode node = ElementNode.FromElement(patient.ToTypedElement());
            TabularMappingDefinition defination = new TabularMappingDefinition("Test");
            defination.Root.Children[""] = new DefinitionNode(new ColumnDefinition("column1", "string", "address.last()", null),
                                                              new ColumnDefinition("column2", "string", "address.first().city", null));
            Dictionary<string, (object, object)> result = t.ToTabular(node, defination);

            Assert.AreEqual("{\"city\":\"Beijing\"}", result["column1"].Item1);
            Assert.AreEqual("Shanghai", result["column2"].Item1);
        }
    }
}
