// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaProvider;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataProcessor
{
    public class ParquetDataProcessorTests
    {
        private static readonly FhirParquetSchemaManager _fhirSchemaManager;
        private static readonly IOptions<ArrowConfiguration> _arrowConfigurationOptions;
        private static readonly NullLogger<ParquetDataProcessor> _nullParquetDataProcessorLogger = NullLogger<ParquetDataProcessor>.Instance;

        private const string _testDataFolder = "./TestData";
        private const string _expectTestDataFolder = "./TestData/Expected";
        private static readonly List<JObject> _testPatients;
        private static readonly JObject _testPatient;

        static ParquetDataProcessorTests()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = TestUtils.DefaultSchemaDirectoryPath,
            });
            
            _fhirSchemaManager = new FhirParquetSchemaManager(schemaConfigurationOption, ParquetSchemaProviderDelegate, NullLogger<FhirParquetSchemaManager>.Instance);
            _arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            _testPatient = TestUtils.LoadNdjsonData(Path.Combine(_testDataFolder, "Basic_Raw_Patient.ndjson")).First();
            _testPatients = new List<JObject> { _testPatient, _testPatient };
        }

        private static IParquetSchemaProvider ParquetSchemaProviderDelegate(string name)
        {
            return new LocalDefaultSchemaProvider(NullLogger<LocalDefaultSchemaProvider>.Instance);
        }

        [Fact]
        public static void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(null, _arrowConfigurationOptions, _nullParquetDataProcessorLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(_fhirSchemaManager, null, _nullParquetDataProcessorLogger));
        }

        [Fact]
        public static async Task GivenAValidInputData_WhenProcess_CorrectResultShouldBeReturned()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(_testPatients);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(_expectTestDataFolder, "Expected_Patient.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        // It may takes few minutes to run this large input data test.
        [Fact(Skip = "test")]
        public static async Task GivenAValidMultipleLargeInputData_WhenProcess_CorrectResultShouldBeReturned()
        {
            var largePatientSingleSet = TestUtils.LoadNdjsonData(Path.Combine(_testDataFolder, "Large_Patient.ndjson"));
            var largeTestData = Enumerable.Repeat(largePatientSingleSet, 100).SelectMany(x => x);

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(largeTestData);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(_expectTestDataFolder, "Expected_Patient_MultipleLargeSize.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        // It may takes few minutes to run this large input data test.
        [Fact]
        public static async Task GivenAValidLargeInputData_WhenProcess_CorrectResultShouldBeReturned()
        {
            var largePatientSingleSet = TestUtils.LoadNdjsonData(Path.Combine(_testDataFolder, "Large_Patient.ndjson"));

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(largePatientSingleSet);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(_expectTestDataFolder, "Expected_Patient_LargeSize.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        [Fact]
        public static async Task GivenDataWithSomeRecordsLengthLargerThanBlockSize_WhenProcess_LargeRecordsShouldBeIgnored()
        {
            var shortPatientData = new JObject
            {
                { "resourceType", "Patient" },
                { "id", "example" },
            };

            var testData = new List<JObject>(_testPatients) { shortPatientData };

            // Set BlockSize small here, only shortPatientData can be retained an be converting to parquet result.
            var arrowConfigurationOptions = Options.Create(new ArrowConfiguration()
            {
                ReadOptions = new ArrowReadOptionsConfiguration() { BlockSize = 50 },
            });

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(testData);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(_expectTestDataFolder, "Expected_Patient_IgnoreLargeLength.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        [Fact]
        public static async Task GivenDataAllRecordsLengthLargerThanBlockSize_WhenProcess_NullResultShouldReturned()
        {
            // Set BlockSize small here, only shortPatientData can be retained an be converting to parquet result.
            var arrowConfigurationOptions = Options.Create(new ArrowConfiguration()
            {
                ReadOptions = new ArrowReadOptionsConfiguration() { BlockSize = 50 },
            });

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var testData = new List<JObject>(_testPatients);
            var jsonBatchData = new JsonBatchData(testData);

            StreamBatchData result = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));
            Assert.Null(result);
        }

        [Fact]
        public static async Task GivenInvalidSchemaType_WhenProcess_ExceptionShouldBeThrown()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(_testPatients);

            await Assert.ThrowsAsync<ParquetDataProcessorException>(
                () => parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("InvalidResourceType")));
        }

        [Fact]
        public static async Task GivenInvalidJsonBatchData_WhenProcess_ExceptionShouldBeThrown()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var invalidTestData = new JObject
            {
                { "resourceType", "Patient" },
                { "name", "Invalid field content, should be an array." },
            };

            var invalidJsonBatchData = new JsonBatchData(new List<JObject> { invalidTestData, invalidTestData });

            await Assert.ThrowsAsync<ParquetDataProcessorException>(() => parquetDataProcessor.ProcessAsync(invalidJsonBatchData, new ProcessParameters("Patient")));
        }

        [Fact]
        public static void GivenAValidBasicSchema_WhenPreprocess_CorrectResultShouldBeReturned()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var result = parquetDataProcessor.Preprocess(
                CreateTestJsonBatchData(_testPatient),
                "Patient");

            var expectedResult = TestUtils.LoadNdjsonData(Path.Combine(_expectTestDataFolder, "Expected_Processed_Patient.ndjson"));

            Assert.True(JToken.DeepEquals(result.Values.First(), expectedResult.First()));
        }

        [Fact]
        public static void GivenAValidStructData_WhenPreprocess_CorrectResultShouldBeReturned()
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

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var result = parquetDataProcessor.Preprocess(
                CreateTestJsonBatchData(rawStructFormatData),
                "Patient");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedStructFormatResult));
        }

        [Fact]
        public static void GivenAValidArrayData_WhenPreprocess_CorrectResultShouldBeReturned()
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

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);
            var result = parquetDataProcessor.Preprocess(
                CreateTestJsonBatchData(rawArrayFormatData),
                "Patient");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedArrayFormatResult));
        }

        [Fact]
        public static void GivenAValidDataWithDeepArrayField_WhenPreprocess_DeepFieldsShouldBeWrappedIntoJsonString()
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

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);
            var result = parquetDataProcessor.Preprocess(
                CreateTestJsonBatchData(rawDeepFieldsData),
                "Patient");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedJsonStringFieldsResult));
        }

        [Fact]
        public static void GivenAValidDataWithDeepStructField_WhenPreprocess_DeepFieldsShouldBeWrappedIntoJsonString()
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

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);
            var result = parquetDataProcessor.Preprocess(
                CreateTestJsonBatchData(rawDeepFieldsData),
                "Patient");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedJsonStringFieldsResult));
        }

        [Fact]
        public static void GivenAValidPrimitiveChoiceTypeData_WhenPreprocess_CorrectResultShouldBeReturned()
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

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);
            var result = parquetDataProcessor.Preprocess(
                CreateTestJsonBatchData(rawPrimitiveChoiceTypeData),
                "Observation");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedPrimitiveChoiceTypeResult));
        }

        [Fact]
        public static void GivenAValidStructChoiceTypeData_WhenPreprocess_CorrectResultShouldBeReturned()
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

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);
            var result = parquetDataProcessor.Preprocess(
                CreateTestJsonBatchData(rawStructChoiceTypeData),
                "Observation");
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedStructChoiceTypeResult));
        }

        [Fact]
        public static void GivenInvalidSchemaType_WhenPreprocess_ExceptionShouldBeReturned()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            Assert.Throws<ParquetDataProcessorException>(
                () => parquetDataProcessor.Preprocess(
                    CreateTestJsonBatchData(_testPatient),
                    "UnsupportedSchemaType"));
        }

        [Fact]
        public static void GivenInvalidData_WhenPreprocess_ExceptionShouldBeReturned()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);
            var invalidFieldData = new JObject
            {
                { "name", "Invalid data fields, should be array." },
            };

            Assert.Throws<ParquetDataProcessorException>(
                () => parquetDataProcessor.Preprocess(
                    CreateTestJsonBatchData(invalidFieldData),
                    "Patient").Values.Count());

            Assert.Throws<ParquetDataProcessorException>(
                () => parquetDataProcessor.Preprocess(
                    CreateTestJsonBatchData(null),
                    "Patient").Values.Count());
        }

        private static JsonBatchData CreateTestJsonBatchData(JObject testJObjectData)
        {
            var testData = new List<JObject>() { testJObjectData };
            return new JsonBatchData(testData);
        }

        private static MemoryStream GetExpectedParquetStream(string filePath)
        {
            var expectedResult = new MemoryStream();
            using var file = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            file.CopyTo(expectedResult);

            return expectedResult;
        }
    }
}
