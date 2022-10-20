// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet.SchemaProvider
{
    public class LocalDefaultSchemaProviderTests
    {
        private static readonly LocalDefaultSchemaProvider _testLocalDefaultSchemaProvider;

        static LocalDefaultSchemaProviderTests()
        {
            _testLocalDefaultSchemaProvider = new LocalDefaultSchemaProvider(
                Options.Create(new FhirServerConfiguration()),
                new DiagnosticLogger(),
                NullLogger<LocalDefaultSchemaProvider>.Instance);
        }

        [InlineData("Patient", 24)]
        [InlineData("Observation", 32)]
        [InlineData("Encounter", 31)]
        [InlineData("Claim", 35)]
        [Theory]
        public static async void GivenSchemaDirectory_WhenGetSchema_CorrectResultShouldBeReturned(string schemaType, int propertyCount)
        {
            var defaultSchemas = await _testLocalDefaultSchemaProvider.GetSchemasAsync();

            Assert.Equal(145, defaultSchemas.Count);

            Assert.Equal(schemaType, defaultSchemas[schemaType].Name);
            Assert.False(defaultSchemas[schemaType].IsLeaf);
            Assert.Equal(propertyCount, defaultSchemas[schemaType].SubNodes.Count);
        }
    }
}
