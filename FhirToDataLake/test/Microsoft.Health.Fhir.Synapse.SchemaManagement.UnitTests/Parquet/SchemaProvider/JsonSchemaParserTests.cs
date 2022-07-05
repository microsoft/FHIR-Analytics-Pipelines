// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet.SchemaProvider
{
    public class JsonSchemaParserTests
    {
        public static IEnumerable<object[]> GetInvalidJsonSchemaContents()
        {
            yield return new object[] { JSchema.Parse(File.ReadAllText(Path.Join(TestConstants.CustomizedTestSchemaDirectory, "SchemaWithInvalidPropertyType.schema.json"))) };
            yield return new object[] { JSchema.Parse(File.ReadAllText(Path.Join(TestConstants.CustomizedTestSchemaDirectory, "SchemaWithInvalidType.schema.json"))) };
            yield return new object[] { JSchema.Parse(File.ReadAllText(Path.Join(TestConstants.CustomizedTestSchemaDirectory, "SchemaWithoutPropertyType.schema.json"))) };
            yield return new object[] { JSchema.Parse(File.ReadAllText(Path.Join(TestConstants.CustomizedTestSchemaDirectory, "SchemaWithoutType.schema.json"))) };
        }

        [Fact]
        public void GivenAJsonSchema_WhenParseJSchema_CorrectResultShouldBeReturned()
        {
            var testSchema = JSchema.Parse(File.ReadAllText(Path.Join(TestConstants.CustomizedTestSchemaDirectory, "ValidSchema.schema.json")));
            var parquetSchemaNode = JsonSchemaParser.ParseJSchema("testType", testSchema);
            var expectedSchemaNode = JSchema.Parse(File.ReadAllText(Path.Join(TestConstants.CustomizedTestSchemaDirectory, "ExpectedValidParquetSchemaNode.json")));

            Assert.True(JToken.DeepEquals(
                JObject.Parse(JsonConvert.SerializeObject(parquetSchemaNode)),
                JObject.Parse(JsonConvert.SerializeObject(expectedSchemaNode))));
        }

        [Theory]
        [MemberData(nameof(GetInvalidJsonSchemaContents))]
        public void GivenAInvalidJsonSchema_WhenParseJSchema_ExceptionsShouldBeThrown(JSchema jSchema)
        {
            var schemaParser = new JsonSchemaParser();
            Assert.Throws<GenerateFhirParquetSchemaNodeException>(() => JsonSchemaParser.ParseJSchema("testType", jSchema));
        }
    }
}
