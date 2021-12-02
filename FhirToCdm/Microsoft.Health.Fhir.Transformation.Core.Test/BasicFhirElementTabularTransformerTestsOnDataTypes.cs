// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Health.Fhir.Transformation.Core.Test
{
    [TestClass]
    public class BasicFhirElementTabularTransformerTestsOnDataTypes
    {
        [TestMethod]
        public void GivenAFhirDateNode_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new Date("2015").ToTypedElement()),  (new DateTime(2015, 1, 1), "date")),
                (node: ElementNode.FromElement(new Date("2015-05").ToTypedElement()),  (new DateTime(2015, 5, 1), "date")),
                (node: ElementNode.FromElement(new Date("2015-05-04").ToTypedElement()),  (new DateTime(2015, 5, 4), "date")),
                (node: null,  (null, "date"))
            };
            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                var transformResult = transformer.ConvertElementNode(example.node, FhirTypeNames.Date);
                Assert.AreEqual(transformResult, example.Item2);
            }
        }

        [TestMethod]
        public void GivenAFhirDateTimeNode_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new FhirDateTime("2015").ToTypedElement()),  (new DateTime(2015, 1, 1), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2015-05").ToTypedElement()),  (new DateTime(2015, 5, 1), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2015-05-04").ToTypedElement()),  (new DateTime(2015, 5, 4), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2016-05-04T12:00:00").ToTypedElement()),  (new DateTime(2016, 5, 4, 12, 0, 0), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2016-05-04T12:00:00+00:00").ToTypedElement()),  (new DateTime(2016, 5, 4, 12, 0, 0), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2016-05-04T12:00:00Z").ToTypedElement()),  (new DateTime(2016, 5, 4, 12, 0, 0), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2015-02-07T13:28:17").ToTypedElement()),  (new DateTime(2015, 2, 7, 13, 28, 17), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2015-02-07T13:28:17.159").ToTypedElement()),  (new DateTime(2015, 2, 7, 13, 28, 17, 159), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2015-02-07T19:28:17-07:00").ToTypedElement()),  (new DateTime(2015, 2, 8, 2, 28, 17), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2015-02-07T13:28:17.159-05:00").ToTypedElement()),  (new DateTime(2015, 2, 7, 18, 28, 17, 159), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2015-02-07T13:28:17Z").ToTypedElement()),  (new DateTime(2015, 2, 7, 13, 28, 17), "dateTime")),
                (node: ElementNode.FromElement(new FhirDateTime("2015-02-07T13:28:17.159Z").ToTypedElement()),  (new DateTime(2015, 2, 7, 13, 28, 17, 159), "dateTime")),
                (node: null,  (null, "dateTime"))
            };
            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                var transformResult = transformer.ConvertElementNode(example.node, FhirTypeNames.DateTime);
                Assert.AreEqual(transformResult, example.Item2);
            }
        }

        [TestMethod]
        public void GivenAFhirInstantNode_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new Instant(DateTimeOffset.Parse("2015-05-04Z")).ToTypedElement()),  (new DateTime(2015, 5, 4), "instant")),
                (node: ElementNode.FromElement(new Instant(DateTimeOffset.Parse("2015-02-07T13:28:17Z")).ToTypedElement()),  (new DateTime(2015, 2, 7, 13, 28, 17), "instant")),
                (node: ElementNode.FromElement(new Instant(DateTimeOffset.Parse("2015-02-07T13:28:17.159Z")).ToTypedElement()),  (new DateTime(2015, 2, 7, 13, 28, 17, 159), "instant")),
                (node: ElementNode.FromElement(new Instant(DateTimeOffset.Parse("2015-02-07T13:28:17-05:00")).ToTypedElement()),  (new DateTime(2015, 2, 7, 18, 28, 17), "instant")),
                (node: ElementNode.FromElement(new Instant(DateTimeOffset.Parse("2015-02-07T13:28:17.159-05:00")).ToTypedElement()),  (new DateTime(2015, 2, 7, 18, 28, 17, 159), "instant")),
                (node: ElementNode.FromElement(new Instant(DateTimeOffset.Parse("2015-02-07T13:28:17Z")).ToTypedElement()),  (new DateTime(2015, 2, 7, 13, 28, 17), "instant")),
                (node: ElementNode.FromElement(new Instant(DateTimeOffset.Parse("2015-02-07T13:28:17.159Z")).ToTypedElement()),  (new DateTime(2015, 2, 7, 13, 28, 17, 159), "instant")),
                (node: null,  (null, "instant"))
            };
            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                var transformResult = transformer.ConvertElementNode(example.node, FhirTypeNames.Instant);
                Assert.AreEqual(transformResult, example.Item2);
            }
        }

        [TestMethod]
        public void GivenAFhirTimeNode_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new Time("01:00:00").ToTypedElement()),  (new DateTime(0001, 1, 1, 1, 0, 0), "time")),
                (node: ElementNode.FromElement(new Time("12:00:00").ToTypedElement()),  (new DateTime(0001, 1, 1, 12, 0, 0), "time")),
                (node: ElementNode.FromElement(new Time("00:00:00").ToTypedElement()),  (new DateTime(0001, 1, 1, 0, 0, 0), "time")),
                (node: ElementNode.FromElement(new Time("05:01:00.159").ToTypedElement()),  (new DateTime(0001, 1, 1, 5, 1, 0, 159), "time")),
                (node: null,  (null, "time"))
            };
            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                var transformResult = transformer.ConvertElementNode(example.node, FhirTypeNames.Time);
                Assert.AreEqual(transformResult, example.Item2);
            }
        }

        [TestMethod]
        public void GivenAFhirDecimalNode_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new FhirDecimal((decimal)1).ToTypedElement()), ((float)(1), "decimal")),
                (node: ElementNode.FromElement(new FhirDecimal((decimal)1.1).ToTypedElement()), ((float)(1.1), "decimal")),
                (node: ElementNode.FromElement(new FhirDecimal((decimal)1.1e5).ToTypedElement()), ((float)110000, "decimal")),
                (node: null, (null, "decimal"))
            };

            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                var transformResult = transformer.ConvertElementNode(example.node, FhirTypeNames.Decimal);
                Assert.AreEqual(transformResult, example.Item2);
            }
        }

        [TestMethod]
        public void GivenAFhirIntegerNode_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new Integer(int.MaxValue).ToTypedElement()), (int.MaxValue, "integer")),
                (node: ElementNode.FromElement(new Integer(+1).ToTypedElement()), ((int)(+1), "integer")),
                (node: ElementNode.FromElement(new Integer(-1).ToTypedElement()), ((int)(-1), "integer")),
                (node: null, (null, "integer"))
            };

            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                var transformResult = transformer.ConvertElementNode(example.node, FhirTypeNames.Integer);
                Assert.AreEqual(transformResult, example.Item2);
            }
        }

        [TestMethod]
        public void GivenAFhirPositiveIntNode_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new PositiveInt(int.MaxValue).ToTypedElement()), (int.MaxValue, "positiveInt")),
                (node: ElementNode.FromElement(new PositiveInt(0).ToTypedElement()), ((int)(0), "positiveInt")),
                (node: ElementNode.FromElement(new PositiveInt(-1).ToTypedElement()), ((int)(-1), "positiveInt")),
                (node: null, (null, "positiveInt"))
            };

            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                var transformResult = transformer.ConvertElementNode(example.node, FhirTypeNames.PositiveInt);
                Assert.AreEqual(transformResult, example.Item2);
            }
        }

        [TestMethod]
        public void GivenAFhirUnsignedIntNode_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new UnsignedInt(int.MaxValue).ToTypedElement()), (int.MaxValue, "unsignedInt")),
                (node: ElementNode.FromElement(new UnsignedInt(0).ToTypedElement()), ((int)(0), "unsignedInt")),
                (node: ElementNode.FromElement(new UnsignedInt(-1).ToTypedElement()), ((int)(-1), "unsignedInt")),
                (node: null, (null, "unsignedInt"))
            };

            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                var transformResult = transformer.ConvertElementNode(example.node, FhirTypeNames.UnsignedInt);
                Assert.AreEqual(transformResult, example.Item2);
            }
        }

        [TestMethod]
        public void GivenAFhirBase64BinaryNode_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new Base64Binary(Encoding.ASCII.GetBytes("A stream of bytes, base64 encoded")).ToTypedElement()),
                                               (new string("A stream of bytes, base64 encoded"), "base64Binary")),
                (node: ElementNode.FromElement(new Base64Binary(Encoding.ASCII.GetBytes("")).ToTypedElement()), (new string(""), "base64Binary")),
                (node: null, (null, "base64Binary"))
            };

            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                string expectedString = example.Item2.expectedValue == null? null
                                        :Convert.ToBase64String(Encoding.ASCII.GetBytes(example.Item2.expectedValue.ToString()));
                var transformResult = transformer.ConvertElementNode(example.node, FhirTypeNames.Base64Binary);
                Assert.AreEqual(transformResult, (expectedString, example.Item2.expectedType));
            }
        }

        [TestMethod]
        public void GivenStringRelated_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            List<(ElementNode node, (object expectedValue, object expectedType))> examples = new List<(ElementNode node, (object expectedValue, object expectedType))>() {
                (node: ElementNode.FromElement(new FhirString("").ToTypedElement()),
                                (new string(""), "string")),
                (node: ElementNode.FromElement(new FhirString("A sequence of Unicode characters").ToTypedElement()),
                                (new string("A sequence of Unicode characters"), "string")),
                (node: ElementNode.FromElement(new Canonical("A URI that refers to a resource by its canonical URL").ToTypedElement()),
                                (new string("A URI that refers to a resource by its canonical URL"), "canonical")),
                (node: ElementNode.FromElement(new Code("A date, or partial date as used in human communication").ToTypedElement()),
                                (new string("A date, or partial date as used in human communication"), "code")),
                (node: ElementNode.FromElement(new Id("Any combination of upper- or lower-case ASCII letters").ToTypedElement()),
                                (new string("Any combination of upper- or lower-case ASCII letters"), "id")),
                (node: ElementNode.FromElement(new Markdown("A FHIR string that may contain markdown").ToTypedElement()),
                                (new string("A FHIR string that may contain markdown"), "markdown")),
                (node: ElementNode.FromElement(new Oid("urn:oid:1.2.3.4.5").ToTypedElement()),
                                (new string("urn:oid:1.2.3.4.5"), "oid")),
                (node: ElementNode.FromElement(new Uuid("urn:uuid:c757873d-ec9a-4326-a141-556f43239520").ToTypedElement()),
                                (new string("urn:uuid:c757873d-ec9a-4326-a141-556f43239520"), "uuid")),
                (node: ElementNode.FromElement(new FhirUri("https://www.hl7.org/fhir").ToTypedElement()),
                                (new string("https://www.hl7.org/fhir"), "uri")),
                (node: ElementNode.FromElement(new FhirUrl("https://www.hl7.org/fhir").ToTypedElement()),
                                (new string("https://www.hl7.org/fhir"), "url"))
        };

            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            foreach (var example in examples)
            {
                var transformResult = transformer.ConvertElementNode(example.node, example.Item2.expectedType.ToString());
                Assert.AreEqual(transformResult, example.Item2);
            }
        }

        [TestMethod]
        public void GivenAFhirArrayNodeThatHasParent_AfterTransformingUsingAsJson_CorrectStringInstanceShouldBeReturned()
        {
            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();

            Patient patient = new Patient() { Gender = AdministrativeGender.Male };
            patient.Name.Add(new HumanName() { Use = HumanName.NameUse.Official, Family = "Chalmers", Given = new List<string>() { "Peter", "James" } });
            patient.Name.Add(new HumanName() { Use = HumanName.NameUse.Usual, Given = new List<string>() { "Jim" } });
            patient.Name.Add(new HumanName() { Use = HumanName.NameUse.Maiden, Family = "Windsor", Given = new List<string>() { "Peter" } });
            ElementNode node = ElementNode.FromElement(patient.ToTypedElement());

            var expected = ("[{\"use\":\"official\",\"family\":\"Chalmers\",\"given\":[\"Peter\",\"James\"]},{\"use\":\"usual\",\"given\":[\"Jim\"]},{\"use\":\"maiden\",\"family\":\"Windsor\",\"given\":[\"Peter\"]}]"
                              ,"array");
            Assert.AreEqual(transformer.ConvertElementNode(node["name"].FirstOrDefault(), FhirTypeNames.Array), expected);
        }

        [TestMethod]
        public void GivenANodeWithInvalidValue_AfterTransforming_CorrectDateTimeInstanceShouldBeReturned()
        {
            FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();
            ElementNode node = ElementNode.FromElement(new Date("").ToTypedElement());
            Assert.AreEqual(transformer.ConvertElementNode(node, FhirTypeNames.Date),
                         (null, "date"));

            node.Value = null;
            Assert.AreEqual(transformer.ConvertElementNode(node, FhirTypeNames.Date),
                         (null, "date"));

            node.Value = "123";
            Assert.AreEqual(transformer.ConvertElementNode(node, FhirTypeNames.Date),
                         (null, "date"));
        }

    }
}
