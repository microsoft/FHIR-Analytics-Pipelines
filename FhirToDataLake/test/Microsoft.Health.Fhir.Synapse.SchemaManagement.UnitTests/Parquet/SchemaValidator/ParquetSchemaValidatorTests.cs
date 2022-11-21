// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaValidator;
using Newtonsoft.Json;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet.SchemaValidator
{
    public class ParquetSchemaValidatorTests
    {
        public static IEnumerable<object[]> GetInvalidParquetSchemaContent()
        {
            yield return new object[]
            {
                File.ReadAllText(Path.Join(TestUtils.InvalidParquetSchemaDirectory, "Invalid_schema_invalid_subnodes.json")),
                "The leaf schema node 'testType.name' shouldn't have sub nodes.",
            };
            yield return new object[]
            {
                File.ReadAllText(Path.Join(TestUtils.InvalidParquetSchemaDirectory, "Invalid_schema_no_subnodes.json")),
                "The schema node 'testType.resourceType' sub nodes cannot be null.",
            };
            yield return new object[]
            {
                File.ReadAllText(Path.Join(TestUtils.InvalidParquetSchemaDirectory, "Invalid_schema_leaf_type_not_primitive1.json")),
                "The leaf schema node 'testType.resourceType' type 'Patient' is not primitive.",
            };
            yield return new object[]
            {
                File.ReadAllText(Path.Join(TestUtils.InvalidParquetSchemaDirectory, "Invalid_schema_leaf_type_not_primitive2.json")),
                "The leaf schema node 'testType.resourceType' type 'not exist type' is not primitive.",
            };
            yield return new object[]
            {
                File.ReadAllText(Path.Join(TestUtils.InvalidParquetSchemaDirectory, "Invalid_schema_leaf_type_not_primitive3.json")),
                "The leaf schema node 'testType.resourceType' type 'Patient' is not primitive.",
            };
            yield return new object[]
            {
                File.ReadAllText(Path.Join(TestUtils.InvalidParquetSchemaDirectory, "Invalid_schema_leaf_type_not_primitive4.json")),
                "The leaf schema node 'testType.name.use' type 'Patient' is not primitive.",
            };
        }

        public static IEnumerable<object[]> GetValidParquetSchemaContent()
        {
            yield return new object[] { File.ReadAllText(Path.Join(TestUtils.ExampleParquetSchemaDirectory, "Patient.json")) };
            yield return new object[] { File.ReadAllText(Path.Join(TestUtils.ExampleParquetSchemaDirectory, "Observation.json")) };
            yield return new object[] { File.ReadAllText(Path.Join(TestUtils.ExampleParquetSchemaDirectory, "Patient_customized.json")) };
        }

        [Theory]
        [MemberData(nameof(GetInvalidParquetSchemaContent))]
        public void GivenInvalidParquetSchemaNode_WhenValidate_FalseShouldBeReturned(string schemaContent, string expectedError)
        {
            var schemaNode = JsonConvert.DeserializeObject<ParquetSchemaNode>(schemaContent);
            ValidationResult validateResult = ParquetSchemaValidator.Validate("testType", schemaNode);
            Assert.False(validateResult.Success);
            Assert.Equal(expectedError, validateResult.ErrorMessage);
        }

        [Theory]
        [MemberData(nameof(GetValidParquetSchemaContent))]
        public void GivenValidParquetSchemaNode_WhenValidate_TrueShouldBeReturned(string schemaContent)
        {
            var schemaNode = JsonConvert.DeserializeObject<ParquetSchemaNode>(schemaContent);
            ValidationResult validateResult = ParquetSchemaValidator.Validate("testType", schemaNode);
            Assert.True(validateResult.Success);
            Assert.Equal(string.Empty, validateResult.ErrorMessage);
        }

        [Fact]
        public void GivenNullSchemaNode_WhenValidate_FalseShouldBeReturned()
        {
            ValidationResult validateResult = ParquetSchemaValidator.Validate("testType", null);
            Assert.False(validateResult.Success);
            Assert.Equal("The schema node 'testType' cannot be null.", validateResult.ErrorMessage);
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(null)]
        public void GivenInvalidSchemaKey_WhenValidate_FalseShouldBeReturned(string invalidSchemaKey)
        {
            var schemaContent = File.ReadAllText(Path.Join(TestUtils.ExampleParquetSchemaDirectory, "Patient.json"));
            var schemaNode = JsonConvert.DeserializeObject<ParquetSchemaNode>(schemaContent);
            ValidationResult validateResult = ParquetSchemaValidator.Validate(invalidSchemaKey, schemaNode);
            Assert.False(validateResult.Success);
            Assert.Equal("The schema key cannot be null or empty.", validateResult.ErrorMessage);
        }
    }
}
