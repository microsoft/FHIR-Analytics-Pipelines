// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NJsonSchema;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet.SchemaProvider
{
    public class JsonSchemaParserTests
    {
        public static IEnumerable<object[]> GetInvalidJsonSchemaContents()
        {
            yield return new object[] { JsonSchema.FromJsonAsync(File.ReadAllText(Path.Join(TestUtils.CustomizedTestSchemaDirectory, "SchemaWithInvalidPropertyType.schema.json"))).GetAwaiter().GetResult() };
            yield return new object[] { JsonSchema.FromJsonAsync(File.ReadAllText(Path.Join(TestUtils.CustomizedTestSchemaDirectory, "SchemaWithInvalidType.schema.json"))).GetAwaiter().GetResult() };
            yield return new object[] { JsonSchema.FromJsonAsync(File.ReadAllText(Path.Join(TestUtils.CustomizedTestSchemaDirectory, "SchemaWithoutPropertyType.schema.json"))).GetAwaiter().GetResult() };
            yield return new object[] { JsonSchema.FromJsonAsync(File.ReadAllText(Path.Join(TestUtils.CustomizedTestSchemaDirectory, "SchemaWithoutType.schema.json"))).GetAwaiter().GetResult() };
        }

        [Fact]
        public void GivenAJsonSchema_WhenParseJSchema_CorrectResultShouldBeReturned()
        {
            JsonSchema testSchema = JsonSchema.FromJsonAsync(File.ReadAllText(Path.Join(TestUtils.CustomizedTestSchemaDirectory, "ValidSchema.schema.json"))).GetAwaiter().GetResult();
            FhirParquetSchemaNode parquetSchemaNode = JsonSchemaParser.ParseJSchema("testType", testSchema);
            JsonSchema expectedSchemaNode = JsonSchema.FromJsonAsync(File.ReadAllText(Path.Join(TestUtils.ExpectedDataDirectory, "ExpectedValidParquetSchemaNode.json"))).GetAwaiter().GetResult();

            Assert.True(JToken.DeepEquals(
                JObject.Parse(JsonConvert.SerializeObject(parquetSchemaNode)),
                JObject.Parse(JsonConvert.SerializeObject(expectedSchemaNode))));
        }

        [Theory]
        [MemberData(nameof(GetInvalidJsonSchemaContents))]
        public void GivenAInvalidJsonSchema_WhenParseJSchema_ExceptionsShouldBeThrown(JsonSchema jSchema)
        {
            JsonSchemaParser schemaParser = new JsonSchemaParser();
            Assert.Throws<GenerateFhirParquetSchemaNodeException>(() => JsonSchemaParser.ParseJSchema("testType", jSchema));
        }
    }
}
