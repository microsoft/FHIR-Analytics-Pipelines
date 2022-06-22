// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.CustomizedSchema;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet.CustomizedSchema
{
    public class JsonSchemaParserTests
    {
        private static readonly string _customizedSchemaDirectory = "../../../TestData/CustomizedSchema";

        public static IEnumerable<object[]> GetInvalidJsonSchemaContents()
        {
            yield return new object[] { JSchema.Parse(File.ReadAllText(Path.Join(_customizedSchemaDirectory, "SchemaWithInvalidPropertyType.schema.json"))) };
            yield return new object[] { JSchema.Parse(File.ReadAllText(Path.Join(_customizedSchemaDirectory, "SchemaWithInvalidType.schema.json"))) };
            yield return new object[] { JSchema.Parse(File.ReadAllText(Path.Join(_customizedSchemaDirectory, "SchemaWithoutPropertyType.schema.json"))) };
            yield return new object[] { JSchema.Parse(File.ReadAllText(Path.Join(_customizedSchemaDirectory, "SchemaWithoutType.schema.json"))) };
        }

        [Fact]
        public void GivenAJsonSchema_WhenParseJSchema_CorrectResultShouldBeReturned()
        {
            var testSchema = JSchema.Parse(File.ReadAllText(Path.Join(_customizedSchemaDirectory, "ValidSchema.schema.json")));
            var schemaParser = new JsonSchemaParser();
            var parquetSchemaNode = schemaParser.ParseJSchema("testType", testSchema);

            Assert.True(JToken.DeepEquals(
                JObject.Parse(JsonConvert.SerializeObject(parquetSchemaNode)),
                JObject.Parse(File.ReadAllText(Path.Join(_customizedSchemaDirectory, "ExpectedValidParquetSchemaNode.json")))));
        }

        [Theory]
        [MemberData(nameof(GetInvalidJsonSchemaContents))]
        public void GivenAInvalidJsonSchema_WhenParseJSchema_ExceptionsShouldBeThrown(JSchema jSchema)
        {
            var schemaParser = new JsonSchemaParser();
            Assert.Throws<ParseJsonSchemaException>(() => schemaParser.ParseJSchema("testType", jSchema));
        }
    }
}
