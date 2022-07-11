// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet
{
    public class FhirParquetSchemaManagerTests
    {
        private static readonly FhirParquetSchemaManager _testParquetSchemaManager;

        static FhirParquetSchemaManagerTests()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = TestConstants.DefaultSchemaDirectory,
            });

            _testParquetSchemaManager = new FhirParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, NullLogger<FhirParquetSchemaManager>.Instance);
        }

        private static IParquetSchemaProvider ParquetSchemaProviderDelegate(string name)
        {
            return new LocalDefaultSchemaProvider(NullLogger<LocalDefaultSchemaProvider>.Instance);
        }

        [InlineData("Patient", 24)]
        [InlineData("Observation", 32)]
        [InlineData("Encounter", 31)]
        [InlineData("Claim", 35)]
        [Theory]
        public static void GivenASchemaType_WhenGetSchema_CorrectResultShouldBeReturned(string schemaType, int propertyCount)
        {
            var result = _testParquetSchemaManager.GetSchema(schemaType);

            Assert.Equal(schemaType, result.Name);
            Assert.False(result.IsLeaf);
            Assert.Equal(propertyCount, result.SubNodes.Count);
        }

        [Fact]
        public static void GivenInvalidSchemaType_WhenGetSchema_NullShouldBeReturned()
        {
            var schemaType = "InvalidSchemaType";

            var result = _testParquetSchemaManager.GetSchema(schemaType);
            Assert.Null(result);
        }

        [Fact]
        public static void WhenGetAllSchemas_CorrectResultShouldBeReturned()
        {
            var schemas = _testParquetSchemaManager.GetAllSchemas();
            Assert.Equal(145, schemas.Count);
        }

        [InlineData("Patient")]
        [InlineData("Observation")]
        [InlineData("Encounter")]
        [InlineData("Claim")]
        [Theory]
        public static void GivenAResourceType_WhenGetSchemaTypes_CorrectResultShouldBeReturned(string resourceType)
        {
            var schemaTypes = _testParquetSchemaManager.GetSchemaTypes(resourceType);
            Assert.Single(schemaTypes);
            Assert.Equal(resourceType, schemaTypes[0]);
        }

        [Fact]
        public static void GivenInvalidResourceType_WhenGetSchemaTypes_EmptyResultShouldBeReturned()
        {
            var resourceType = "InvalidResourceType";

            var schemaTypes = _testParquetSchemaManager.GetSchemaTypes(resourceType);
            Assert.Empty(schemaTypes);
        }
    }
}
