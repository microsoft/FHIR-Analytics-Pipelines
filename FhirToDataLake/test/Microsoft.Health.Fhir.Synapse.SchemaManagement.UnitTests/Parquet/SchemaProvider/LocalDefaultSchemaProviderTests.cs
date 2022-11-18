// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet.SchemaProvider
{
    public class LocalDefaultSchemaProviderTests
    {
        private static readonly LocalDefaultSchemaProvider _testLocalDefaultR4SchemaProvider;

        static LocalDefaultSchemaProviderTests()
        {
            _testLocalDefaultR4SchemaProvider = new LocalDefaultSchemaProvider(
                Options.Create(new DataSourceConfiguration()),
                new DiagnosticLogger(),
                NullLogger<LocalDefaultSchemaProvider>.Instance);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            var dataSourceOption = Options.Create(new DataSourceConfiguration());
            var diagnosticLogger = new DiagnosticLogger();
            var loggerInstance = NullLogger<LocalDefaultSchemaProvider>.Instance;

            Assert.Throws<ArgumentNullException>(
                () => new LocalDefaultSchemaProvider(null, diagnosticLogger, loggerInstance));

            Assert.Throws<ArgumentNullException>(
                () => new LocalDefaultSchemaProvider(dataSourceOption, null, loggerInstance));

            Assert.Throws<ArgumentNullException>(
                () => new LocalDefaultSchemaProvider(dataSourceOption, diagnosticLogger, null));
        }

        [InlineData("Patient", 24)]
        [InlineData("Observation", 32)]
        [InlineData("Encounter", 31)]
        [InlineData("Claim", 35)]
        [Theory]
        public static async void GivenSchemaDirectory_WhenGetR4Schema_CorrectResultShouldBeReturned(string schemaType, int propertyCount)
        {
            var defaultR4Schemas = await _testLocalDefaultR4SchemaProvider.GetSchemasAsync();

            Assert.Equal(145, defaultR4Schemas.Count);

            Assert.Equal(schemaType, defaultR4Schemas[schemaType].Name);
            Assert.False(defaultR4Schemas[schemaType].IsLeaf);
            Assert.Equal(propertyCount, defaultR4Schemas[schemaType].SubNodes.Count);
        }

        [InlineData("Patient", 24)]
        [InlineData("Observation", 33)]
        [InlineData("Encounter", 33)]
        [InlineData("Citation", 38)]
        [Theory]
        public static async void GivenSchemaDirectory_WhenGetR5Schema_CorrectResultShouldBeReturned(string schemaType, int propertyCount)
        {
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                FhirServer = new FhirServerConfiguration
                {
                    Version = FhirVersion.R5,
                },
            };

            var schemaProvider = new LocalDefaultSchemaProvider(
                Options.Create(dataSourceConfiguration),
                new DiagnosticLogger(),
                NullLogger<LocalDefaultSchemaProvider>.Instance);

            Dictionary<string, FhirParquetSchemaNode> defaultR5Schemas = await schemaProvider.GetSchemasAsync();

            Assert.Equal(151, defaultR5Schemas.Count);

            Assert.Equal(schemaType, defaultR5Schemas[schemaType].Name);
            Assert.False(defaultR5Schemas[schemaType].IsLeaf);
            Assert.Equal(propertyCount, defaultR5Schemas[schemaType].SubNodes.Count);
        }
    }
}
