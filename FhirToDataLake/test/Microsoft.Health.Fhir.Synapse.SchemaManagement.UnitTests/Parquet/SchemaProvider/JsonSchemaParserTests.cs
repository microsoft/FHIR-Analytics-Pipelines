// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NJsonSchema;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet.SchemaProvider
{
    public class JsonSchemaParserTests
    {
        private static readonly JsonSchema _testSchema;

        static JsonSchemaParserTests()
        {
            _testSchema = JsonSchema.FromJsonAsync(File.ReadAllText(Path.Join(TestUtils.CustomizedTestSchemaDirectory, "ValidSchema.schema.json"))).GetAwaiter().GetResult();
        }

        public static IEnumerable<object[]> GetInvalidJsonSchemaObjects()
        {
            yield return new object[]
            {
                new JObject
                {
                    { "title", "Invalid schema without root type" },
                    {
                        "properties", new JObject
                        {
                            { "resourceType", new JObject { { "type", "string" } } },
                            { "id", new JObject { { "type", "string" } } },
                        }
                    },
                },
            };

            yield return new object[]
            {
                new JObject
                {
                    { "title", "Invalid schema without property type" },
                    { "type", "object" },
                    {
                        "properties", new JObject
                        {
                            { "resourceType", new JObject { } },
                            { "id", new JObject { { "type", "string" } } },
                        }
                    },
                },
            };

            yield return new object[]
            {
                new JObject
                {
                    { "title", "Invalid schema root type must be object" },
                    { "type", "array" },
                    {
                        "properties", new JObject
                        {
                            { "resourceType", new JObject { { "type", "string" } } },
                            { "id", new JObject { { "type", "string" } } },
                        }
                    },
                },
            };

            yield return new object[]
            {
                new JObject
                {
                    { "title", "Invalid schema property type must be primitive" },
                    {
                        "properties", new JObject
                        {
                            { "resourceType", new JObject { { "type", "array" } } },
                            { "id", new JObject { { "type", "string" } } },
                        }
                    },
                },
            };
            yield return new object[]
            {
                new JObject
                {
                    { "title", "Invalid schema property type must be primitive" },
                    {
                        "properties", new JObject
                        {
                            { "resourceType", new JObject { { "type", "object" } } },
                            { "id", new JObject { { "type", "string" } } },
                        }
                    },
                },
            };
        }

        [Fact]
        public void GivenAJsonSchema_WhenParseJSchema_CorrectResultShouldBeReturned()
        {
            var parquetSchemaNode = JsonSchemaParser.ParseJSchema("testType", _testSchema);
            var expectedSchemaNode = JsonSchema.FromJsonAsync(File.ReadAllText(Path.Join(TestUtils.ExpectedDataDirectory, "ExpectedValidParquetSchemaNode.json"))).GetAwaiter().GetResult();

            var parquetSchemaContent = JsonConvert.SerializeObject(parquetSchemaNode);
            var expectedSchemaContent = JsonConvert.SerializeObject(expectedSchemaNode);

            Assert.True(JToken.DeepEquals(
                JObject.Parse(JsonConvert.SerializeObject(parquetSchemaNode)),
                JObject.Parse(JsonConvert.SerializeObject(expectedSchemaNode))));
        }

        [Theory]
        [MemberData(nameof(GetInvalidJsonSchemaObjects))]
        public void GivenAInvalidJsonSchema_WhenParseJSchema_ExceptionsShouldBeThrown(JObject schemaJObject)
        {
            var jSchema = JsonSchema.FromJsonAsync(schemaJObject.ToString()).GetAwaiter().GetResult();

            var schemaParser = new JsonSchemaParser();
            Assert.Throws<GenerateFhirParquetSchemaNodeException>(() => JsonSchemaParser.ParseJSchema("testType", jSchema));
        }

        [Fact]
        public void GivenNullParameters_WhenParseJSchema_ExceptionsShouldBeThrown()
        {
            Assert.Throws<ArgumentException>(() => JsonSchemaParser.ParseJSchema(string.Empty, _testSchema));
            Assert.Throws<ArgumentException>(() => JsonSchemaParser.ParseJSchema(" ", _testSchema));
            Assert.Throws<ArgumentNullException>(() => JsonSchemaParser.ParseJSchema(null, _testSchema));
            Assert.Throws<ArgumentNullException>(() => JsonSchemaParser.ParseJSchema("testType", null));
        }
    }
}
