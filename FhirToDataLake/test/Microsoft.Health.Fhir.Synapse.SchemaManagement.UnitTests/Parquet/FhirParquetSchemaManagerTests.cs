// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using NSubstitute;
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
                SchemaCollectionDirectory = "../../../../../data/schemas",
            });
            var jsonSchemaCollectionsProvider = new JsonSchemaCollectionProvider(Substitute.For<IContainerRegistryTokenProvider>(), NullLogger<JsonSchemaCollectionProvider>.Instance);

            _testParquetSchemaManager = new FhirParquetSchemaManager(schemaConfigurationOption, jsonSchemaCollectionsProvider, NullLogger<FhirParquetSchemaManager>.Instance);
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

        [InlineData("")]
        [InlineData(null)]
        [InlineData("../../../TestData/InvalidSchemas/NoSchemaDirectory")]
        [InlineData("../../../TestData/InvalidSchemas/NoSchemaType")]
        [InlineData("../../../TestData/InvalidSchemas/InvalidSchemaFile")]
        [Theory]
        public static void GivenInvalidSchema_WhenInitialize_ExceptionShouldBeThrown(string schemaDirectoryPath)
        {
            var invalidSchemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = schemaDirectoryPath,
            });
            var jsonSchemaCollectionsProvider = new JsonSchemaCollectionProvider(Substitute.For<IContainerRegistryTokenProvider>(), NullLogger<JsonSchemaCollectionProvider>.Instance);


            Assert.Throws<FhirSchemaException>(
                () => new FhirParquetSchemaManager(invalidSchemaConfigurationOption, jsonSchemaCollectionsProvider, NullLogger<FhirParquetSchemaManager>.Instance));
        }
    }
}
