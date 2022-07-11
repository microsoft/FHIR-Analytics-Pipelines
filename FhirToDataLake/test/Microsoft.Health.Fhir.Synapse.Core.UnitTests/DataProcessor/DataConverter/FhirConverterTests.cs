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
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataProcessor.DataConverter
{
    public class FhirConverterTests
    {
        private static readonly FhirConverter _testFhirConverter;

        static FhirConverterTests()
        {
            _testFhirConverter = new FhirConverter(TestUtils.GetTestAcrTemplateProvider(), NullLogger<FhirConverter>.Instance);
        }

        [Fact]
        public static void GivenAValidJsonBatchData_WhenConvert_CorrectResultShouldBeReturned()
        {
            var testData = File.ReadLines(Path.Join(TestUtils.TestDataFolder, "Basic_Raw_Patient.ndjson"))
                        .Select(dataContent => JObject.Parse(dataContent));
            var result = _testFhirConverter.Convert(new JsonBatchData(testData), "Patient").Values.ToList();

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
            var testData = File.ReadLines(Path.Join(TestUtils.TestDataFolder, "Basic_Raw_Patient.ndjson"))
                        .Select(dataContent => JObject.Parse(dataContent));

            Assert.Throws<ParquetDataProcessorException>(() => _testFhirConverter.Convert(new JsonBatchData(testData), "Invalid_patient"));
        }

        [Fact]
        public static void GivenNullJsonBatchData_WhenConvert_ExceptionShouldBeThrown()
        {
            Assert.Throws<ParquetDataProcessorException>(() => _testFhirConverter.Convert(null, "Patient"));
            Assert.Throws<ParquetDataProcessorException>(() => _testFhirConverter.Convert(new JsonBatchData(null), "Patient"));
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
