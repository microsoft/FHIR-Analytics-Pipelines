// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataProcessor.DataConverter
{
    public class DefaultSchemaConverterTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private static readonly JObject _testPatient;
        private static readonly DefaultSchemaConverter _testDefaultConverter;

        static DefaultSchemaConverterTests()
        {
            IOptions<SchemaConfiguration> schemaConfigurationOption = Options.Create(new SchemaConfiguration());

            var schemaManager = new FhirParquetSchemaManager(
                schemaConfigurationOption,
                TestUtils.TestParquetSchemaProviderDelegate,
                NullLogger<FhirParquetSchemaManager>.Instance);

            _testDefaultConverter = new DefaultSchemaConverter(schemaManager, _diagnosticLogger, NullLogger<DefaultSchemaConverter>.Instance);
            _testPatient = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.TestDataFolder, "Basic_Raw_Patient.ndjson")).First();
        }

        [Fact]
        public void GivenAValidBasicSchema_WhenConvert_CorrectResultShouldBeReturned()
        {
            JsonBatchData result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(_testPatient),
                "Patient");

            IEnumerable<JObject> expectedResult = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Processed_Patient.ndjson"));

            Assert.True(JToken.DeepEquals(result.Values.First(), expectedResult.First()));
        }

        [Fact]
        public void GivenAValidStructData_WhenConvert_CorrectResultShouldBeReturned()
        {
            var rawStructFormatData = new JObject
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
            var expectedStructFormatResult = new JObject
            {
                {
                    "text", new JObject
                    {
                        { "status", "generated" },
                        { "div", "Test div in text" },
                    }
                },
            };

            JsonBatchData result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawStructFormatData),
                "Patient");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedStructFormatResult));
        }

        [Fact]
        public void GivenAValidArrayData_WhenConvert_CorrectResultShouldBeReturned()
        {
            var rawArrayFormatData = new JObject
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
            var expectedArrayFormatResult = new JObject
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

            JsonBatchData result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawArrayFormatData),
                "Patient");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedArrayFormatResult));
        }

        [Fact]
        public void GivenAValidDataWithDeepArrayField_WhenConvert_DeepFieldsShouldBeWrappedIntoJsonString()
        {
            var rawDeepFieldsData = new JObject
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

            var expectedJsonStringFieldsResult = new JObject
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

            JsonBatchData result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawDeepFieldsData),
                "Patient");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedJsonStringFieldsResult));
        }

        [Fact]
        public void GivenAValidDataWithDeepStructField_WhenConvert_DeepFieldsShouldBeWrappedIntoJsonString()
        {
            var rawDeepFieldsData = new JObject
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

            var expectedJsonStringFieldsResult = new JObject
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

            JsonBatchData result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawDeepFieldsData),
                "Patient");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedJsonStringFieldsResult));
        }

        [Fact]
        public void GivenAValidPrimitiveChoiceTypeData_WhenConvert_CorrectResultShouldBeReturned()
        {
            var rawPrimitiveChoiceTypeData = new JObject
            {
                { "effectiveDateTime", "1905-08-23" },
            };

            // Primitive choice data type
            var expectedPrimitiveChoiceTypeResult = new JObject
            {
                { "effective", new JObject { { "dateTime", "1905-08-23" } } },
            };

            JsonBatchData result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawPrimitiveChoiceTypeData),
                "Observation");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedPrimitiveChoiceTypeResult));
        }

        [Fact]
        public void GivenAValidStructChoiceTypeData_WhenConvert_CorrectResultShouldBeReturned()
        {
            var rawStructChoiceTypeData = new JObject
            {
                { "effectivePeriod", new JObject { { "start", "1905-08-23" } } },
            };

            // Struct choice data type
            var expectedStructChoiceTypeResult = new JObject
            {
                { "effective", new JObject { { "period", new JObject { { "start", "1905-08-23" } } } } },
            };

            JsonBatchData result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(rawStructChoiceTypeData),
                "Observation");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedStructChoiceTypeResult));
        }

        [Fact]
        public void GivenNullschema_WhenConvert_ExceptionShouldBeReturned()
        {
            Assert.Throws<ArgumentNullException>(
                () => _testDefaultConverter.Convert(
                    CreateTestJsonBatchData(_testPatient),
                    null));

            Assert.Throws<ArgumentNullException>(
                () => _testDefaultConverter.Convert(
                    null,
                    "Observation"));
        }

        [Fact]
        public void GivenInvalidData_WhenConvert_ExceptionShouldBeReturned()
        {
            var invalidFieldData = new JObject
            {
                { "name", "Invalid data fields, should be array." },
            };

            Assert.Throws<ParquetDataProcessorException>(()
                => _testDefaultConverter.Convert(CreateTestJsonBatchData(invalidFieldData), "Patient").Values.Count());

            Assert.Throws<ParquetDataProcessorException>(()
                => _testDefaultConverter.Convert(CreateTestJsonBatchData(null), "Patient").Values.Count());
        }

        private static JsonBatchData CreateTestJsonBatchData(JObject testJObjectData)
        {
            List<JObject> testData = new List<JObject>() { testJObjectData };
            return new JsonBatchData(testData);
        }
    }
}
