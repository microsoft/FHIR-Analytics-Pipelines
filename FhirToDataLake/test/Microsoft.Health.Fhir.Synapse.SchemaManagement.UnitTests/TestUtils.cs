// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using DotLiquid;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Microsoft.Health.Fhir.TemplateManagement;
using NSubstitute;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests
{
    public static class TestUtils
    {
        public const string PipelineDefaultSchemaDirectoryPath = "../../../../../data/schemas";

        public const string TestDataDirectory = "../../../TestData";

        public const string ExpectedDataDirectory = TestDataDirectory + "/Expected";
        public const string CustomizedTestSchemaDirectory = TestDataDirectory + "/CustomizedSchema";
        public const string TestJsonSchemaFilePath = CustomizedTestSchemaDirectory + "/ValidSchema.schema.json";
        public const string TestTemplateTarGzPath = TestDataDirectory + "/TemplateTest.tar.gz";

        public const string MockSchemaImageReference = "testacr.azurecr.io/customizedtemplate:default";

        public static IContainerRegistryTokenProvider GetMockAcrTokenProvider(string accessToken)
        {
            var tokenProvider = Substitute.For<IContainerRegistryTokenProvider>();

            tokenProvider.GetTokenAsync(default, default).ReturnsForAnyArgs($"Basic {accessToken}");
            return tokenProvider;
        }

        public static IContainerRegistryTemplateProvider GetMockAcrTemplateProvider(List<Dictionary<string, Template>> templateCollections)
        {
            var templateProvider = Substitute.For<IContainerRegistryTemplateProvider>();
            templateProvider.GetTemplateCollectionAsync(default, default).ReturnsForAnyArgs(templateCollections);
            return templateProvider;
        }

        public static string GetAcrAccessToken(string serverUsername, string serverPassword)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes($"{serverUsername}:{serverPassword}"));
        }

        public static List<Dictionary<string, Template>> GetSchemaTemplateCollections(string schemaKey, byte[] schemaContent)
        {
            var schemaContents = new Dictionary<string, byte[]> { { schemaKey, schemaContent } };

            var templateCollection = TemplateLayerParser.ParseToTemplates(schemaContents);
            return new List<Dictionary<string, Template>> { templateCollection };
        }

        public static IParquetSchemaProvider TestParquetSchemaProviderDelegate(string name)
        {
            var testSchemaTemplateCollections = TestUtils.GetSchemaTemplateCollections("Schema/Patient.schema.json", File.ReadAllBytes(TestUtils.TestJsonSchemaFilePath));

            if (name == FhirParquetSchemaConstants.DefaultSchemaProviderKey)
            {
                return new LocalDefaultSchemaProvider(NullLogger<LocalDefaultSchemaProvider>.Instance);
            }
            else
            {
                return new AcrCustomizedSchemaProvider(GetMockAcrTemplateProvider(testSchemaTemplateCollections), NullLogger<AcrCustomizedSchemaProvider>.Instance);
            }
        }
    }
}
