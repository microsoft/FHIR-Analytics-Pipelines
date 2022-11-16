﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using DotLiquid;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Microsoft.Health.Fhir.TemplateManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NSubstitute;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests
{
    public static class TestUtils
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        public const string TestDataFolder = "./TestData";
        public const string ExpectTestDataFolder = TestDataFolder + "/Expected";
        public const string TestNormalSchemaDirectoryPath = TestDataFolder + "/Schema";
        public const string TestInvalidSchemaDirectoryPath = TestDataFolder + "/InvalidSchema";
        public const string TestCustomizedSchemaDirectoryPath = TestDataFolder + "/CustomizedSchema";
        public const string TestFilterTarGzPath = TestDataFolder + "/filter.tar.gz";

        public static readonly SchemaConfiguration TestDefaultSchemaConfiguration = new SchemaConfiguration();
        public static readonly SchemaConfiguration TestCustomSchemaConfiguration = new SchemaConfiguration()
        {
            EnableCustomizedSchema = true,
            SchemaImageReference = "testacr.azurecr.io/customizedtemplate:default",
        };

        public static readonly List<string> ExcludeResourceTypes = new List<string>
        {
            FhirConstants.StructureDefinitionResource,
            FhirConstants.OperationOutcomeResource,
        };

        public static IEnumerable<JObject> LoadNdjsonData(string filePath)
        {
            // Date formatted strings are not parsed to a date type and are read as strings,
            // to prevent output from being affected by time zone.
            var serializerSettings = new JsonSerializerSettings { DateParseHandling = DateParseHandling.None };

            foreach (string line in File.ReadAllLines(filePath))
            {
                yield return JsonConvert.DeserializeObject<JObject>(line, serializerSettings);
            }
        }

        public static IContainerRegistryTemplateProvider GetMockAcrTemplateProvider()
        {
            Dictionary<string, byte[]> templateContents = Directory.GetFiles(TestCustomizedSchemaDirectoryPath, "*", SearchOption.AllDirectories)
                .Select(filePath =>
                {
                    byte[] templateContent = File.ReadAllBytes(filePath);
                    return new KeyValuePair<string, byte[]>(Path.GetRelativePath(TestCustomizedSchemaDirectoryPath, filePath), templateContent);
                }).ToDictionary(x => x.Key, x => x.Value);

            Dictionary<string, Template> templateCollection = TemplateLayerParser.ParseToTemplates(templateContents);

            var templateProvider = Substitute.For<IContainerRegistryTemplateProvider>();
            templateProvider.GetTemplateCollectionAsync(default, default).ReturnsForAnyArgs(new List<Dictionary<string, Template>> { templateCollection });
            return templateProvider;
        }

        public static IParquetSchemaProvider TestParquetSchemaProviderDelegate(string name)
        {
            if (name == FhirParquetSchemaConstants.DefaultSchemaProviderKey)
            {
                return new LocalDefaultSchemaProvider(
                    Options.Create(new FhirServerConfiguration()),
                    _diagnosticLogger,
                    NullLogger<LocalDefaultSchemaProvider>.Instance);
            }
            else
            {
                return new AcrCustomizedSchemaProvider(
                    GetMockAcrTemplateProvider(),
                    Options.Create(TestCustomSchemaConfiguration),
                    _diagnosticLogger,
                    NullLogger<AcrCustomizedSchemaProvider>.Instance);
            }
        }

        public static IDataSchemaConverter TestDataSchemaConverterDelegate(string name)
        {
            var fhirSchemaManagerWithoutCustomizedSchema = new FhirParquetSchemaManager(
                Options.Create(new SchemaConfiguration()),
                TestParquetSchemaProviderDelegate,
                _diagnosticLogger,
                NullLogger<FhirParquetSchemaManager>.Instance);

            if (name == FhirParquetSchemaConstants.DefaultSchemaProviderKey)
            {
                return new DefaultSchemaConverter(
                    fhirSchemaManagerWithoutCustomizedSchema,
                    _diagnosticLogger,
                    NullLogger<DefaultSchemaConverter>.Instance);
            }
            else
            {
                return new CustomSchemaConverter(
                    GetMockAcrTemplateProvider(),
                    Options.Create(TestCustomSchemaConfiguration),
                    _diagnosticLogger,
                    NullLogger<CustomSchemaConverter>.Instance);
            }
        }
    }
}
