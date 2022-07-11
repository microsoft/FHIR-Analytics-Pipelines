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
    public class DefaultConverterTests
    {
        private static readonly JObject _testPatient;
        private static readonly DefaultConverter _testDefaultConverter;
        private static readonly FhirParquetSchemaManager _schemaManager;

        static DefaultConverterTests()
        {

            var schemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = TestUtils.PipelineDefaultSchemaDirectoryPath,
            });

            _testDefaultConverter = new DefaultConverter(NullLogger<DefaultConverter>.Instance);
            _schemaManager = new FhirParquetSchemaManager(schemaConfigurationOption, TestUtils.GetTestParquetSchemaProviderDelegate, NullLogger<FhirParquetSchemaManager>.Instance);
            _testPatient = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.TestDataFolder, "Basic_Raw_Patient.ndjson")).First();
        }

        [Fact]
        public static void GivenAValidBasicSchema_WhenConvert_CorrectResultShouldBeReturned()
        {
            var result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(_testPatient),
                _schemaManager.GetSchema("Patient"));

            var expectedResult = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Processed_Patient.ndjson"));

            Assert.True(JToken.DeepEquals(result.Values.First(), expectedResult.First()));
        }

        [Fact]
        public static void GivenAValidStructData_WhenConvert_CorrectResultShouldBeReturned()
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

            var result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawStructFormatData),
                _schemaManager.GetSchema("Patient"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedStructFormatResult));
        }

        [Fact]
        public static void GivenAValidArrayData_WhenConvert_CorrectResultShouldBeReturned()
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

            var result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawArrayFormatData),
                _schemaManager.GetSchema("Patient"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedArrayFormatResult));
        }

        [Fact]
        public static void GivenAValidDataWithDeepArrayField_WhenConvert_DeepFieldsShouldBeWrappedIntoJsonString()
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

            var result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawDeepFieldsData),
                _schemaManager.GetSchema("Patient"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedJsonStringFieldsResult));
        }

        [Fact]
        public static void GivenAValidDataWithDeepStructField_WhenConvert_DeepFieldsShouldBeWrappedIntoJsonString()
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

            var result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawDeepFieldsData),
                _schemaManager.GetSchema("Patient"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedJsonStringFieldsResult));
        }

        [Fact]
        public static void GivenAValidPrimitiveChoiceTypeData_WhenConvert_CorrectResultShouldBeReturned()
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

            var result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawPrimitiveChoiceTypeData),
                _schemaManager.GetSchema("Observation"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedPrimitiveChoiceTypeResult));
        }

        [Fact]
        public static void GivenAValidStructChoiceTypeData_WhenConvert_CorrectResultShouldBeReturned()
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

            var result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawStructChoiceTypeData),
                _schemaManager.GetSchema("Observation"));
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedStructChoiceTypeResult));
        }

        [Fact]
        public static void GivenNullschema_WhenConvert_ExceptionShouldBeReturned()
        {
            Assert.Throws<ParquetDataProcessorException>(
                () => _testDefaultConverter.Convert(
                    CreateTestJsonBatchData(_testPatient),
                    null));

            Assert.Throws<ParquetDataProcessorException>(
                () => _testDefaultConverter.Convert(
                    null,
                    _schemaManager.GetSchema("Observation")));
        }

        [Fact]
        public static void GivenInvalidData_WhenConvert_ExceptionShouldBeReturned()
        {
            var invalidFieldData = new JObject
            {
                { "name", "Invalid data fields, should be array." },
            };

            Assert.Throws<ParquetDataProcessorException>(
                () => _testDefaultConverter.Convert(
                    CreateTestJsonBatchData(invalidFieldData),
                    _schemaManager.GetSchema("Patient"))
                .Values.Count());

            Assert.Throws<ParquetDataProcessorException>(
                () => _testDefaultConverter.Convert(
                    CreateTestJsonBatchData(null),
                    _schemaManager.GetSchema("Patient"))
                .Values.Count());
        }

        private static JsonBatchData CreateTestJsonBatchData(JObject testJObjectData)
        {
            var testData = new List<JObject>() { testJObjectData };
            return new JsonBatchData(testData);
        }
    }
}
