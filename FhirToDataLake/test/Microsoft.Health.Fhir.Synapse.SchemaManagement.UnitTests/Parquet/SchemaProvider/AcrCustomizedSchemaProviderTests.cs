// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.IO;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using NJsonSchema;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.Parquet.SchemaProvider
{
    public class AcrCustomizedSchemaProviderTests
    {
        private static readonly SchemaConfiguration _schemaConfigurationWithCustomizedSchema;

        static AcrCustomizedSchemaProviderTests()
        {
            _schemaConfigurationWithCustomizedSchema = new SchemaConfiguration()
            {
                EnableCustomizedSchema = true,
                SchemaImageReference = TestUtils.MockSchemaImageReference,
            };
        }

        [Fact]
        public static async void GivenImageReference_WhenGetSchemaWithMockTemplateProvider_CorrectResultShouldBeReturned()
        {
            var testSchemaTemplateCollections = TestUtils.GetSchemaTemplateCollections("Schema/Patient.schema.json", File.ReadAllBytes(TestUtils.TestJsonSchemaFilePath));

            var schemaProvider = new AcrCustomizedSchemaProvider(
                TestUtils.GetMockAcrTemplateProvider(testSchemaTemplateCollections),
                Options.Create(_schemaConfigurationWithCustomizedSchema),
                new DiagnosticLogger(),
                NullLogger<AcrCustomizedSchemaProvider>.Instance);

            var schemaCollections = await schemaProvider.GetSchemasAsync();
            var expectedSchemaNode = JsonSchemaParser.ParseJSchema("Patient", JsonSchema.FromJsonAsync(File.ReadAllText(TestUtils.TestJsonSchemaFilePath)).GetAwaiter().GetResult());

            Assert.Equal(expectedSchemaNode.Name, schemaCollections["Patient_Customized"].Name);
        }

        [Theory]
        [InlineData("Schema/subDir/Patient.schema.json")]
        public static async void GivenImageReference_WhenGetSchemaWithInvalidTemplateProvider_CorrectResultShouldBeReturned(string schemaKey)
        {
            var testSchemaTemplateCollections = TestUtils.GetSchemaTemplateCollections(schemaKey, File.ReadAllBytes(TestUtils.TestJsonSchemaFilePath));
            var schemaProvider = new AcrCustomizedSchemaProvider(
                TestUtils.GetMockAcrTemplateProvider(testSchemaTemplateCollections),
                Options.Create(_schemaConfigurationWithCustomizedSchema),
                new DiagnosticLogger(),
                NullLogger<AcrCustomizedSchemaProvider>.Instance);

            await Assert.ThrowsAsync<ContainerRegistrySchemaException>(() => schemaProvider.GetSchemasAsync());
        }
    }
}
