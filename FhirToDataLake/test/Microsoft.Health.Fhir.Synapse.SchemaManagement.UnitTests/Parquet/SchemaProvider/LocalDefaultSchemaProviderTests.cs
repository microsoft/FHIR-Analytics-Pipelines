// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet.SchemaProvider
{
    public class LocalDefaultSchemaProviderTests
    {
        private static readonly LocalDefaultSchemaProvider _testLocalDefaultSchemaProvider;

        static LocalDefaultSchemaProviderTests()
        {
            _testLocalDefaultSchemaProvider = new LocalDefaultSchemaProvider(NullLogger<LocalDefaultSchemaProvider>.Instance);
        }

        [InlineData("Patient", 24)]
        [InlineData("Observation", 32)]
        [InlineData("Encounter", 31)]
        [InlineData("Claim", 35)]
        [Theory]
        public static async void GivenSchemaDirectory_WhenGetSchema_CorrectResultShouldBeReturned(string schemaType, int propertyCount)
        {
            var defaultSchemas = await _testLocalDefaultSchemaProvider.GetSchemasAsync(TestUtils.PipelineDefaultSchemaDirectoryPath);

            Assert.Equal(145, defaultSchemas.Count);

            Assert.Equal(schemaType, defaultSchemas[schemaType].Name);
            Assert.False(defaultSchemas[schemaType].IsLeaf);
            Assert.Equal(propertyCount, defaultSchemas[schemaType].SubNodes.Count);
        }

        [InlineData("")]
        [InlineData(null)]
        [InlineData("../../../TestData/InvalidSchemas/NoSchemaDirectory")]
        [InlineData("../../../TestData/InvalidSchemas/NoSchemaType")]
        [InlineData("../../../TestData/InvalidSchemas/InvalidSchemaFile")]
        [Theory]
        public static async void GivenInvalidSchema_WhenInitialize_ExceptionShouldBeThrown(string schemaDirectoryPath)
        {
            await Assert.ThrowsAsync<GenerateFhirParquetSchemaNodeException>(
                () => _testLocalDefaultSchemaProvider.GetSchemasAsync(schemaDirectoryPath));
        }
    }
}
