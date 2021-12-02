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
using Microsoft.Health.Fhir.Synapse.DataSerialization.Json;
using Microsoft.Health.Fhir.Synapse.DataSerialization.Json.Exceptions;
using Microsoft.Health.Fhir.Synapse.Schema;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataSerialization.UnitTests.Json
{
    public class JsonDataProcessorTests
    {
        private static readonly FhirSchemaManager _fhirSchemaManager;
        private static readonly JObject _patientTestObject;
        private static readonly NullLogger<JsonDataProcessor> _nullLogger = NullLogger<JsonDataProcessor>.Instance;

        private const string _testDataFolder = "./Json/TestData";
        private const string _expectTestDataFolder = "./Json/TestData/Expected";

        static JsonDataProcessorTests()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration() 
            {
                SchemaCollectionDirectory = TestUtils.SchemaDirectoryPath,
            });

            _fhirSchemaManager = new FhirSchemaManager(schemaConfigurationOption, NullLogger<FhirSchemaManager>.Instance);
            _patientTestObject = TestUtils.LoadNdjsonData(Path.Combine(_testDataFolder, "Basic_Raw_Patient.ndjson")).First();
        }

        [Fact]
        public static async void GivenAValidBasicSchema_WhenProcess_CorrectResultShouldBeReturned()
        {
            JsonDataProcessor jsonDataProcessor = new JsonDataProcessor(_fhirSchemaManager, _nullLogger);

            var result = await jsonDataProcessor.ProcessAsync(
                CreateTestJsonBatchData(_patientTestObject),
                TestUtils.GetTestTaskContext("Patient"));

            var expectedResult = TestUtils.LoadNdjsonData(Path.Combine(_expectTestDataFolder, "Expected_Processed_Patient.ndjson"));

            Assert.True(JToken.DeepEquals(result.Values.First(), expectedResult.First()));
        }

        [Fact]
        public static async void GivenAValidStructData_WhenProcess_CorrectResultShouldBeReturned()
        {
            JObject rawStructFormatData = new JObject
            {
                {
                    "text", new JObject
                    {
                        { "status", "generated" },
                        { "div", "Test div in text" },
                    }
                },
            };

            // Expected struct format fields are same with raw struct format fields.
            JObject expectedStructFormatResult = new JObject
            {
                {
                    "text", new JObject
                    {
                        { "status", "generated" },
                        { "div", "Test div in text" },
                    }
                },
            };

            JsonDataProcessor jsonDataProcessor = new JsonDataProcessor(_fhirSchemaManager, _nullLogger);

            var result = await jsonDataProcessor.ProcessAsync(
                CreateTestJsonBatchData(rawStructFormatData),
                TestUtils.GetTestTaskContext("Patient"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedStructFormatResult));
        }

        [Fact]
        public static async void GivenAValidArrayData_WhenProcess_CorrectResultShouldBeReturned()
        {
            JObject rawArrayFormatData = new JObject
            {
                {
                    "name", new JArray
                    {
                        new JObject
                        {
                            { "use", "official" },
                            { "family", "Chalmers" },
                            { "given", new JArray { "Peter", "James" } },
                        },
                        new JObject
                        {
                            { "use", "maiden" },
                            { "given", new JArray { "Jim" } },
                        },
                    }
                },
            };

            // Expected array format fields are same with raw array format fields.
            JObject expectedArrayFormatResult = new JObject
            {
                {
                    "name", new JArray
                    {
                        new JObject
                        {
                            { "use", "official" },
                            { "family", "Chalmers" },
                            { "given", new JArray { "Peter", "James" } },
                        },
                        new JObject
                        {
                            { "use", "maiden" },
                            { "given", new JArray { "Jim" } },
                        },
                    }
                },
            };

            JsonDataProcessor jsonDataProcessor = new JsonDataProcessor(_fhirSchemaManager, _nullLogger);
            var result = await jsonDataProcessor.ProcessAsync(
                CreateTestJsonBatchData(rawArrayFormatData),
                TestUtils.GetTestTaskContext("Patient"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedArrayFormatResult));
        }

        [Fact]
        public static async void GivenAValidData_WithDeepArrayField_WhenProcess_DeepFieldsShouldBeWrappedIntoJsonString()
        {
            JObject rawDeepFieldsData = new JObject
            {
                {
                    "contact", new JArray
                    {
                        new JObject
                        {
                            {
                                "relationship", new JArray
                                {
                                    new JObject
                                    {
                                        {
                                            "coding", new JArray
                                            {
                                                new JObject
                                                {
                                                    { "system", "http://terminology.hl7.org/CodeSystem/v2-0131" },
                                                    { "code", "E" },
                                                },
                                            }
                                        },
                                    },
                                }
                            },
                        },
                    }
                },
            };

            JObject expectedJsonStringFieldsResult = new JObject
            {
                {
                    "contact", new JArray
                    {
                        new JObject
                        {
                            {
                                "relationship", new JArray
                                {
                                    new JObject
                                    {
                                        {
                                            "coding", "[{\"system\":\"http://terminology.hl7.org/CodeSystem/v2-0131\",\"code\":\"E\"}]"
                                        },
                                    },
                                }
                            },
                        },
                    }
                },
            };

            JsonDataProcessor jsonDataProcessor = new JsonDataProcessor(_fhirSchemaManager, _nullLogger);
            var result = await jsonDataProcessor.ProcessAsync(
                CreateTestJsonBatchData(rawDeepFieldsData),
                TestUtils.GetTestTaskContext("Patient"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedJsonStringFieldsResult));
        }

        [Fact]
        public static async void GivenAValidData_WithDeepStructField_WhenProcess_DeepFieldsShouldBeWrappedIntoJsonString()
        {
            JObject rawDeepFieldsData = new JObject
            {
                {
                    "contact", new JArray
                    {
                        new JObject
                        {
                            {
                                "relationship", new JArray
                                {
                                    new JObject
                                    {
                                        {
                                            "coding", new JArray
                                            {
                                                new JObject
                                                {
                                                    { "system", "http://terminology.hl7.org/CodeSystem/v2-0131" },
                                                    { "code", "E" },
                                                },
                                            }
                                        },
                                    },
                                }
                            },
                        },
                    }
                },
            };

            JObject expectedJsonStringFieldsResult = new JObject
            {
                {
                    "contact", new JArray
                    {
                        new JObject
                        {
                            {
                                "relationship", new JArray
                                {
                                    new JObject
                                    {
                                        {
                                            "coding", "[{\"system\":\"http://terminology.hl7.org/CodeSystem/v2-0131\",\"code\":\"E\"}]"
                                        },
                                    },
                                }
                            },
                        },
                    }
                },
            };

            JsonDataProcessor jsonDataProcessor = new JsonDataProcessor(_fhirSchemaManager, _nullLogger);
            var result = await jsonDataProcessor.ProcessAsync(
                CreateTestJsonBatchData(rawDeepFieldsData),
                TestUtils.GetTestTaskContext("Patient"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedJsonStringFieldsResult));
        }

        [Fact]
        public static async void GivenAValidPrimitiveChoiceTypeData_WhenProcess_CorrectResultShouldBeReturned()
        {
            JObject rawPrimitiveChoiceTypeData = new JObject
            {
                { "effectiveDateTime", "1905-08-23" },
            };

            // Primitive choice data type
            JObject expectedPrimitiveChoiceTypeResult = new JObject
            {
                { "effective", new JObject { { "dateTime", "1905-08-23" } } },
            };

            JsonDataProcessor jsonDataProcessor = new JsonDataProcessor(_fhirSchemaManager, _nullLogger);
            var result = await jsonDataProcessor.ProcessAsync(
                CreateTestJsonBatchData(rawPrimitiveChoiceTypeData),
                TestUtils.GetTestTaskContext("Observation"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedPrimitiveChoiceTypeResult));
        }

        [Fact]
        public static async void GivenAValidStructChoiceTypeData_WhenProcess_CorrectResultShouldBeReturned()
        {
            JObject rawStructChoiceTypeData = new JObject
            {
                { "effectivePeriod", new JObject { { "start", "1905-08-23" } } },
            };

            // Struct choice data type
            JObject expectedStructChoiceTypeResult = new JObject
            {
                { "effective", new JObject { { "period", new JObject { { "start", "1905-08-23" } } } } },
            };

            JsonDataProcessor jsonDataProcessor = new JsonDataProcessor(_fhirSchemaManager, _nullLogger);
            var result = await jsonDataProcessor.ProcessAsync(
                CreateTestJsonBatchData(rawStructChoiceTypeData),
                TestUtils.GetTestTaskContext("Observation"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedStructChoiceTypeResult));
        }

        [Fact]
        public static async void GivenUnsupportedResourceType_WhenProcess_ExceptionShouldBeReturned()
        {
            var jsonDataProcessor = new JsonDataProcessor(_fhirSchemaManager, _nullLogger);

            await Assert.ThrowsAsync<JsonDataProcessorException>(
                () => jsonDataProcessor.ProcessAsync(
                    CreateTestJsonBatchData(_patientTestObject),
                    TestUtils.GetTestTaskContext("UnsupportedResouceType")));
        }

        [Fact]
        public static async void GivenInvalidData_WhenProcess_ExceptionShouldBeReturned()
        {
            var jsonDataProcessor = new JsonDataProcessor(_fhirSchemaManager, _nullLogger);
            var invalidFieldData = new JObject
            {
                { "name", "Invalid data fields, should be array." },
            };

            await Assert.ThrowsAsync<JsonDataProcessorException>(
                () => jsonDataProcessor.ProcessAsync(
                    CreateTestJsonBatchData(invalidFieldData),
                    TestUtils.GetTestTaskContext("Patient")));

            await Assert.ThrowsAsync<JsonDataProcessorException>(
                () => jsonDataProcessor.ProcessAsync(
                    CreateTestJsonBatchData(null),
                    TestUtils.GetTestTaskContext("Patient")));
        }

        private static JsonBatchData CreateTestJsonBatchData(JObject testJObjectData)
        {
            var testData = new List<JObject>() { testJObjectData };
            return new JsonBatchData(testData);
        }
    }
}
