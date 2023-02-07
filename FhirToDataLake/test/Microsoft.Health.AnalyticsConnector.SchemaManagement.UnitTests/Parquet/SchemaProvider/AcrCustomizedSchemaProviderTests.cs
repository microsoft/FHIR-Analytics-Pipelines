// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using DotLiquid;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.ContainerRegistry;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Exceptions;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet.SchemaProvider;
using NJsonSchema;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.SchemaManagement.UnitTests.Parquet.SchemaProvider
{
    public class AcrCustomizedSchemaProviderTests
    {
        private static readonly IContainerRegistryTemplateProvider _mockContainerRegistryTemplateProvider;
        private static readonly IOptions<SchemaConfiguration> _schemaConfigurationWithCustomizedSchemaOption;
        private static readonly ILogger<AcrCustomizedSchemaProvider> _nullLogger;
        private static readonly IDiagnosticLogger _diagnosticLogger;

        static AcrCustomizedSchemaProviderTests()
        {
            _diagnosticLogger = new DiagnosticLogger();
            _nullLogger = NullLogger<AcrCustomizedSchemaProvider>.Instance;
            _schemaConfigurationWithCustomizedSchemaOption = Options.Create(new SchemaConfiguration()
            {
                EnableCustomizedSchema = true,
                SchemaImageReference = TestUtils.MockSchemaImageReference,
            });

            var testSchemaTemplateCollections = TestUtils.GetSchemaTemplateCollections("Schema/Patient.schema.json", File.ReadAllBytes(TestUtils.TestJsonSchemaFilePath));
            _mockContainerRegistryTemplateProvider = TestUtils.GetMockAcrTemplateProvider(testSchemaTemplateCollections);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new AcrCustomizedSchemaProvider(null, _schemaConfigurationWithCustomizedSchemaOption, _diagnosticLogger, _nullLogger));

            Assert.Throws<ArgumentNullException>(
                () => new AcrCustomizedSchemaProvider(_mockContainerRegistryTemplateProvider, null, _diagnosticLogger, _nullLogger));

            Assert.Throws<ArgumentNullException>(
                () => new AcrCustomizedSchemaProvider(_mockContainerRegistryTemplateProvider, _schemaConfigurationWithCustomizedSchemaOption, null, _nullLogger));

            Assert.Throws<ArgumentNullException>(
                () => new AcrCustomizedSchemaProvider(_mockContainerRegistryTemplateProvider, _schemaConfigurationWithCustomizedSchemaOption, _diagnosticLogger, null));
        }

        [Fact]
        public static async void GivenImageReference_WhenGetSchemaWithMockTemplateProvider_CorrectResultShouldBeReturned()
        {
            var schemaProvider = new AcrCustomizedSchemaProvider(_mockContainerRegistryTemplateProvider, _schemaConfigurationWithCustomizedSchemaOption, _diagnosticLogger, _nullLogger);

            Dictionary<string, ParquetSchemaNode> schemaCollections = await schemaProvider.GetSchemasAsync();
            ParquetSchemaNode expectedSchemaNode = JsonSchemaParser.ParseJSchema("Patient", JsonSchema.FromJsonAsync(File.ReadAllText(TestUtils.TestJsonSchemaFilePath)).GetAwaiter().GetResult());

            Assert.Equal(expectedSchemaNode.Name, schemaCollections["Patient_Customized"].Name);
        }

        [Theory]
        [InlineData("Schema/subDir/Patient.schema.json")]
        public static async void GivenImageReference_WhenGetSchemaWithInvalidTemplateProvider_CorrectResultShouldBeReturned(string schemaKey)
        {
            List<Dictionary<string, Template>> testSchemaTemplateCollections = TestUtils.GetSchemaTemplateCollections(schemaKey, File.ReadAllBytes(TestUtils.TestJsonSchemaFilePath));
            var schemaProvider = new AcrCustomizedSchemaProvider(
                TestUtils.GetMockAcrTemplateProvider(testSchemaTemplateCollections),
                _schemaConfigurationWithCustomizedSchemaOption,
                _diagnosticLogger,
                _nullLogger);

            await Assert.ThrowsAsync<ContainerRegistrySchemaException>(() => schemaProvider.GetSchemasAsync());
        }

        [Fact]
        public static async void GivenNullImageReference_WhenGetSchemaWithInvalidTemplateProvider_ExceptionResultShouldBeThrown()
        {
            var schemaConfigurationWithCustomizedSchemaOption = Options.Create(new SchemaConfiguration()
            {
                SchemaImageReference = null,
            });

            var schemaProvider = new AcrCustomizedSchemaProvider(
                _mockContainerRegistryTemplateProvider,
                schemaConfigurationWithCustomizedSchemaOption,
                _diagnosticLogger,
                _nullLogger);

            await Assert.ThrowsAsync<ContainerRegistrySchemaException>(() => schemaProvider.GetSchemasAsync());
        }
    }
}
