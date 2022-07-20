// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using DotLiquid;
using Microsoft.Extensions.Logging.Abstractions;
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
        public const string PipelineDefaultSchemaDirectoryPath = "../../../../../data/schemas";

        public const string TestDataFolder = "./TestData";
        public const string ExpectTestDataFolder = TestDataFolder + "/Expected";
        public const string TestNativeSchemaDirectoryPath = TestDataFolder + "/schemas";
        public const string TestCustomizedSchemaDirectoryPath = TestDataFolder + "/CustomizedSchema";

        public static IEnumerable<JObject> LoadNdjsonData(string filePath)
        {
            // Date formatted strings are not parsed to a date type and are read as strings,
            // to prevent output from being affected by time zone.
            var serializerSettings = new JsonSerializerSettings { DateParseHandling = DateParseHandling.None };

            foreach (var line in File.ReadAllLines(filePath))
            {
                yield return JsonConvert.DeserializeObject<JObject>(line, serializerSettings);
            }
        }

        public static IContainerRegistryTemplateProvider GetMockAcrTemplateProvider()
        {
            var templateContents = Directory.GetFiles(TestCustomizedSchemaDirectoryPath, "*", SearchOption.AllDirectories)
                .Select(filePath => 
                {
                    var templateContent = File.ReadAllBytes(filePath);
                    return new KeyValuePair<string, byte[]>(Path.GetRelativePath(TestCustomizedSchemaDirectoryPath, filePath), templateContent);
                }).ToDictionary(x => x.Key, x => x.Value);

            var templateCollection = TemplateLayerParser.ParseToTemplates(templateContents);

            var templateProvider = Substitute.For<IContainerRegistryTemplateProvider>();
            templateProvider.GetTemplateCollectionAsync(default, default).ReturnsForAnyArgs(new List<Dictionary<string, Template>> { templateCollection });
            return templateProvider;
        }

        public static IParquetSchemaProvider TestParquetSchemaProviderDelegate(string name)
        {
            if (name == FhirParquetSchemaConstants.DefaultSchemaProviderKey)
            {
                return new LocalDefaultSchemaProvider(NullLogger<LocalDefaultSchemaProvider>.Instance);
            }
            else
            {
                return new AcrCustomizedSchemaProvider(GetMockAcrTemplateProvider(), NullLogger<AcrCustomizedSchemaProvider>.Instance);
            }
        }
    }
}
