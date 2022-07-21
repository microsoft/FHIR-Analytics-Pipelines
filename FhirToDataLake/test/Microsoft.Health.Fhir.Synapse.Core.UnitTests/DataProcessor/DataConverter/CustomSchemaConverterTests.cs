// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataProcessor.DataConverter
{
    public class CustomSchemaConverterTests
    {
        private static readonly CustomSchemaConverter _testFhirConverter;
        private static readonly JsonBatchData _testData;

        static CustomSchemaConverterTests()
        {
            var schemaConfigurationOptionWithCustomizedSchema = Options.Create(new SchemaConfiguration()
            {
                EnableCustomizedSchema = true,
                SchemaImageReference = "testacr.azurecr.io/customizedtemplate:default",
            });

            _testFhirConverter = new CustomSchemaConverter(
                TestUtils.GetMockAcrTemplateProvider(),
                schemaConfigurationOptionWithCustomizedSchema,
                NullLogger<CustomSchemaConverter>.Instance);

            var testDataContent = File.ReadLines(Path.Join(TestUtils.TestDataFolder, "Basic_Raw_Patient.ndjson"))
                        .Select(dataContent => JObject.Parse(dataContent));

            _testData = new JsonBatchData(testDataContent);
        }

        [Fact]
        public static void GivenAValidJsonBatchData_WhenConvert_CorrectResultShouldBeReturned()
        {
            var result = _testFhirConverter.Convert(_testData, "Patient").Values.ToList();

            var expectedResult = File.ReadLines(Path.Join(TestUtils.ExpectTestDataFolder, "CustomizedSchema/Expected_basic_patient.ndjson"))
                        .Select(dataContent => JObject.Parse(dataContent)).ToList();

            Assert.Equal(expectedResult.Count, result.Count);

            for (int i = 0; i < result.Count; i++)
            {
                Assert.True(JToken.DeepEquals(result[i], expectedResult[i]));
            }
        }

        [Fact]
        public static void GivenInvalidTemplate_WhenConvert_ExceptionShouldBeThrown()
        {
            Assert.Throws<ParquetDataProcessorException>(() => _testFhirConverter.Convert(_testData, "Invalid_patient"));
        }

        [Fact]
        public static void GivenNullOrEmptyInput_WhenConvert_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(() => _testFhirConverter.Convert(null, "Patient"));
            Assert.Throws<ArgumentNullException>(() => _testFhirConverter.Convert(new JsonBatchData(null), "Patient"));
            Assert.Throws<ArgumentNullException>(() => _testFhirConverter.Convert(_testData, null));
            Assert.Throws<ArgumentException>(() => _testFhirConverter.Convert(_testData, string.Empty));
        }

        [Fact]
        public static void GivenInvalidResourceType_WhenPreprocess_ExceptionShouldBeThrown()
        {
            var testData = File.ReadLines(Path.Join(TestUtils.TestDataFolder, "Basic_Raw_Patient.ndjson"))
                    .Select(dataContent => JObject.Parse(dataContent));

            Assert.Throws<ParquetDataProcessorException>(() => _testFhirConverter.Convert(new JsonBatchData(testData), "Invalid resource type"));
        }
    }
}
