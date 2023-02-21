// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Exceptions;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet.SchemaProvider;
using Newtonsoft.Json;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.SchemaManagement.UnitTests.Parquet
{
    public class ParquetSchemaManagerTests
    {
        private static readonly ParquetSchemaManager _testParquetSchemaManagerWithoutCustomizedSchema;
        private static readonly ParquetSchemaManager _testParquetSchemaManagerWithCustomizedSchema;

        private static readonly ILogger<ParquetSchemaManager> _nullLogger;
        private static readonly IDiagnosticLogger _diagnosticLogger;

        static ParquetSchemaManagerTests()
        {
            IOptions<SchemaConfiguration> schemaConfigurationOptionWithoutCustomizedSchema = Options.Create(new SchemaConfiguration());

            IOptions<SchemaConfiguration> schemaConfigurationOptionWithCustomizedSchema = Options.Create(new SchemaConfiguration()
            {
                EnableCustomizedSchema = true,
                SchemaImageReference = TestUtils.MockSchemaImageReference,
            });

            _nullLogger = NullLogger<ParquetSchemaManager>.Instance;
            _diagnosticLogger = new DiagnosticLogger();

            _testParquetSchemaManagerWithoutCustomizedSchema = new ParquetSchemaManager(schemaConfigurationOptionWithoutCustomizedSchema, TestUtils.TestParquetSchemaProviderDelegate, _diagnosticLogger, _nullLogger);
            _testParquetSchemaManagerWithCustomizedSchema = new ParquetSchemaManager(schemaConfigurationOptionWithCustomizedSchema, TestUtils.TestParquetSchemaProviderDelegate, _diagnosticLogger, _nullLogger);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            Assert.Throws<ArgumentNullException>(
                () => new ParquetSchemaManager(null, TestUtils.TestParquetSchemaProviderDelegate, _diagnosticLogger, _nullLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetSchemaManager(schemaConfigurationOption, null, _diagnosticLogger, _nullLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetSchemaManager(schemaConfigurationOption, TestUtils.TestParquetSchemaProviderDelegate, null, _nullLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetSchemaManager(schemaConfigurationOption, TestUtils.TestParquetSchemaProviderDelegate, _diagnosticLogger, null));
        }

        [InlineData("Patient", 34)]
        [InlineData("Observation", 44)]
        [InlineData("Encounter", 35)]
        [InlineData("Claim", 41)]
        [Theory]
        public static void GivenASchemaType_WhenGetSchema_CorrectResultShouldBeReturned(string schemaType, int propertyCount)
        {
            ParquetSchemaNode result = _testParquetSchemaManagerWithoutCustomizedSchema.GetSchema(schemaType);

            Assert.Equal(schemaType, result.Name);
            Assert.False(result.IsLeaf);
            Assert.Equal(propertyCount, result.SubNodes.Count);
        }

        [InlineData("NoneExistSchemaType")]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(null)]
        [Theory]
        public static void GivenInvalidSchemaType_WhenGetSchema_NullShouldBeReturned(string invalidSchemaType)
        {
            ParquetSchemaNode result = _testParquetSchemaManagerWithoutCustomizedSchema.GetSchema(invalidSchemaType);
            Assert.Null(result);
        }

        [Fact]
        public static void WhenGetAllSchemasWithoutCustomizedSchema_CorrectResultShouldBeReturned()
        {
            Dictionary<string, ParquetSchemaNode> schemas = _testParquetSchemaManagerWithoutCustomizedSchema.GetAllSchemas();
            Assert.Equal(145, schemas.Count);
        }

        [Fact]
        public static void WhenGetAllSchemaContentWithoutCustomizedSchema_CorrectResultShouldBeReturned()
        {
            Dictionary<string, string> schemaContent = _testParquetSchemaManagerWithoutCustomizedSchema.GetAllSchemaContent();
            Assert.Equal(145, schemaContent.Count);
        }

        [Fact]
        public static void WhenGetAllSchemasWithCustomizedSchema_CorrectResultShouldBeReturned()
        {
            Dictionary<string, ParquetSchemaNode> schemas = _testParquetSchemaManagerWithCustomizedSchema.GetAllSchemas();

            // Test customized schemas contain a "Patient_Customized" schema.
            Assert.Equal(146, schemas.Count);
        }

        [Fact]
        public static void WhenGetAllSchemaContentWithCustomizedSchema_CorrectResultShouldBeReturned()
        {
            Dictionary<string, string> schemaContent = _testParquetSchemaManagerWithCustomizedSchema.GetAllSchemaContent();

            // Test customized schemas contain a "Patient_Customized" schema.
            Assert.Equal(146, schemaContent.Count);
        }

        [InlineData("Patient")]
        [InlineData("Observation")]
        [InlineData("Encounter")]
        [InlineData("Claim")]
        [Theory]
        public static void GivenAResourceType_WhenGetSchemaTypesWithoutCustomizedSchema_CorrectResultShouldBeReturned(string resourceType)
        {
            List<string> schemaTypes = _testParquetSchemaManagerWithoutCustomizedSchema.GetSchemaTypes(resourceType);
            Assert.Single(schemaTypes);
            Assert.Equal(resourceType, schemaTypes[0]);
        }

        [Fact]
        public static void GivenAResourceType_WhenGetSchemaTypesWithCustomizedSchema_CorrectResultShouldBeReturned()
        {
            List<string> schemaTypes = _testParquetSchemaManagerWithCustomizedSchema.GetSchemaTypes("Patient");
            Assert.Equal(2, schemaTypes.Count);
            Assert.Contains<string>("Patient", schemaTypes);
            Assert.Contains<string>("Patient_Customized", schemaTypes);
        }

        [InlineData("InvalidResourceType")]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(null)]
        [Theory]
        public static void GivenInvalidResourceType_WhenGetSchemaTypes_EmptyResultShouldBeReturned(string invalidResourceType)
        {
            var schemaTypes = _testParquetSchemaManagerWithoutCustomizedSchema.GetSchemaTypes(invalidResourceType);
            Assert.Empty(schemaTypes);
        }

        [Fact]
        public static void GivenInvalidSchema_WhenGetSchemaTypes_ExceptionShouldBeThrown()
        {
            var schemaManager = new ParquetSchemaManager(
                Options.Create(new SchemaConfiguration()),
                ParquetSchemaProviderDelegateWithInvalidSchema,
                _diagnosticLogger,
                _nullLogger);

            Assert.Throws<GenerateFhirParquetSchemaNodeException>(() => schemaManager.GetSchemaTypes("Patient"));
        }

        [Fact]
        public static void GivenInvalidSchema_WhenGetSchema_ExceptionShouldBeThrown()
        {
            var schemaManager = new ParquetSchemaManager(
                Options.Create(new SchemaConfiguration()),
                ParquetSchemaProviderDelegateWithInvalidSchema,
                _diagnosticLogger,
                _nullLogger);

            Assert.Throws<GenerateFhirParquetSchemaNodeException>(() => schemaManager.GetSchema("Patient"));
        }

        [Fact]
        public static void GivenInvalidSchema_WhenGetAllSchemaContent_ExceptionShouldBeThrown()
        {
            var schemaManager = new ParquetSchemaManager(
                Options.Create(new SchemaConfiguration()),
                ParquetSchemaProviderDelegateWithInvalidSchema,
                _diagnosticLogger,
                _nullLogger);

            Assert.Throws<GenerateFhirParquetSchemaNodeException>(() => schemaManager.GetAllSchemaContent());
        }

        [Fact]
        public static void GivenInvalidSchema_WhenGetAllSchemas_ExceptionShouldBeThrown()
        {
            var schemaManager = new ParquetSchemaManager(
                Options.Create(new SchemaConfiguration()),
                ParquetSchemaProviderDelegateWithInvalidSchema,
                _diagnosticLogger,
                _nullLogger);

            Assert.Throws<GenerateFhirParquetSchemaNodeException>(() => schemaManager.GetAllSchemas());
        }

        public static IParquetSchemaProvider ParquetSchemaProviderDelegateWithInvalidSchema(string placeHolderName)
        {
            var invalidParquetSchemaContent = File.ReadAllText(TestUtils.ExampleInvalidParquetSchemaFilePath);
            var invalidParquetSchemaNode = JsonConvert.DeserializeObject<ParquetSchemaNode>(invalidParquetSchemaContent);

            var mockSchemaProvider = Substitute.For<IParquetSchemaProvider>();
            mockSchemaProvider.GetSchemasAsync().Returns(new Dictionary<string, ParquetSchemaNode> { { "testType", invalidParquetSchemaNode } });

            return mockSchemaProvider;
        }
    }
}
