// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Schema.UnitTests
{
    public class FhirSchemaManagerTests
    {
        private static readonly IOptions<SchemaConfiguration> _schemaConfigurationOption;

        static FhirSchemaManagerTests()
        {
            _schemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = "../../../../../data/schemas",
            });
        }

        [InlineData("Patient", 24)]
        [InlineData("Observation", 32)]
        [InlineData("Encounter", 31)]
        [InlineData("Claim", 35)]
        [Theory]
        public static void GivenAResourceType_WhenGetSchema_CorrectResultShouldBeReturned(string resourceType, int propertyCount)
        {
            var schemaManager = new FhirSchemaManager(_schemaConfigurationOption, NullLogger<FhirSchemaManager>.Instance);
            var result = schemaManager.GetSchema(resourceType);

            Assert.Equal(resourceType, result.Name);
            Assert.False(result.IsLeaf);
            Assert.Equal(propertyCount, result.SubNodes.Count);
        }

        [Fact]
        public static void GivenInvalidResourceType_WhenGetSchema_NullShouldBeReturned()
        {
            var schemaManager = new FhirSchemaManager(_schemaConfigurationOption, NullLogger<FhirSchemaManager>.Instance);

            var resourceType = "InvalidResource";

            var result = schemaManager.GetSchema(resourceType);
            Assert.Null(result);
        }

        [Fact]
        public static void WhenGetAllSchemas_CorrectResultShouldBeReturned()
        {
            var schemaManager = new FhirSchemaManager(_schemaConfigurationOption, NullLogger<FhirSchemaManager>.Instance);

            var schemas = schemaManager.GetAllSchemas();
            Assert.Equal(145, schemas.Count);
        }

        [InlineData("")]
        [InlineData(null)]
        [InlineData("../../../InvalidSchemas/NoSchemaDirectory")]
        [InlineData("../../../InvalidSchemas/NoSchemaType")]
        [InlineData("../../../InvalidSchemas/InvalidSchemaFile")]
        [Theory]
        public static void GivenInvalidSchema_WhenInitialize_ExceptionShouldBeThrown(string schemaDirectoryPath)
        {
            var invalidSchemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = schemaDirectoryPath,
            });

            Assert.Throws<FhirSchemaException>(() => new FhirSchemaManager(invalidSchemaConfigurationOption, NullLogger<FhirSchemaManager>.Instance));
        }
    }
}
