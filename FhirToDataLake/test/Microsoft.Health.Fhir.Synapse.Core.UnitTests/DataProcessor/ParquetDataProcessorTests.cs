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
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
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
        private static readonly FhirParquetSchemaManager _fhirSchemaManagerWithoutCustomizedSchema;
        private static readonly FhirParquetSchemaManager _fhirSchemaManagerWithCustomizedSchema;
        private static readonly IOptions<ArrowConfiguration> _arrowConfigurationOptions;
        private static readonly DefaultConverter _defaultConverter;
        private static readonly FhirConverter _fhirConverter;

        private static readonly NullLogger<ParquetDataProcessor> _nullParquetDataProcessorLogger = NullLogger<ParquetDataProcessor>.Instance;

        private static readonly List<JObject> _testPatients;
        private static readonly JObject _testPatient;

        static ParquetDataProcessorTests()
        {
            var schemaConfigurationOptionWithoutCustomizedSchema = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = TestUtils.PipelineDefaultSchemaDirectoryPath,
            });

            _fhirSchemaManagerWithoutCustomizedSchema = new FhirParquetSchemaManager(
                schemaConfigurationOptionWithoutCustomizedSchema,
                TestUtils.TestParquetSchemaProviderDelegate,
                NullLogger<FhirParquetSchemaManager>.Instance);

            var schemaConfigurationOptionWithCustomizedSchema = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = TestUtils.PipelineDefaultSchemaDirectoryPath,
                EnableCustomizedSchema = true,
                ContainerRegistry = new ContainerRegistryConfiguration()
                {
                    SchemaImageReference = "testacr.azurecr.io/customizedtemplate:default",
                },
            });

            _fhirSchemaManagerWithCustomizedSchema = new FhirParquetSchemaManager(
                schemaConfigurationOptionWithCustomizedSchema,
                TestUtils.TestParquetSchemaProviderDelegate,
                NullLogger<FhirParquetSchemaManager>.Instance);

            _defaultConverter = new DefaultConverter(NullLogger<DefaultConverter>.Instance);
            _fhirConverter = new FhirConverter(TestUtils.GetMockAcrTemplateProvider(), NullLogger<FhirConverter>.Instance);

            _arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            _testPatient = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.TestDataFolder, "Basic_Raw_Patient.ndjson")).First();
            _testPatients = new List<JObject> { _testPatient, _testPatient };
        }

        [Fact]
        public static void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(null, _arrowConfigurationOptions, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, null, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, _arrowConfigurationOptions, null, _fhirConverter, _nullParquetDataProcessorLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, _arrowConfigurationOptions, _defaultConverter, null, _nullParquetDataProcessorLogger));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, _arrowConfigurationOptions, _defaultConverter, _fhirConverter, null));
        }

        [Fact]
        public static async Task GivenAValidInputData_WhenProcessWithoutCustomizedSchema_CorrectResultShouldBeReturned()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, _arrowConfigurationOptions, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(_testPatients);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        [Fact]
        public static async Task GivenAValidInputData_WhenProcessWithCustomizedSchema_CorrectResultShouldBeReturned()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManagerWithCustomizedSchema, _arrowConfigurationOptions, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(
                new JsonBatchData(_testPatients),
                new ProcessParameters($"Patient{FhirParquetSchemaConstants.CustomizedSchemaSuffix}"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient_Customized.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        // It may takes few minutes to run this large input data test.
        [Fact]
        public static async Task GivenAValidMultipleLargeInputData_WhenProcess_CorrectResultShouldBeReturned()
        {
            var largePatientSingleSet = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.TestDataFolder, "Large_Patient.ndjson"));
            var largeTestData = Enumerable.Repeat(largePatientSingleSet, 100).SelectMany(x => x);

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, _arrowConfigurationOptions, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(largeTestData);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient_MultipleLargeSize.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        // It may takes few minutes to run this large input data test.
        [Fact]
        public static async Task GivenAValidLargeInputData_WhenProcess_CorrectResultShouldBeReturned()
        {
            var largePatientSingleSet = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.TestDataFolder, "Large_Patient.ndjson"));

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, _arrowConfigurationOptions, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(largePatientSingleSet);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient_LargeSize.parquet"));

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

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, arrowConfigurationOptions, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(testData);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient_IgnoreLargeLength.parquet"));

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

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, arrowConfigurationOptions, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger);

            var testData = new List<JObject>(_testPatients);
            var jsonBatchData = new JsonBatchData(testData);

            StreamBatchData result = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient"));
            Assert.Null(result);
        }

        [Fact]
        public static async Task GivenInvalidSchemaOrResourceType_WhenProcess_ExceptionShouldBeThrown()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, _arrowConfigurationOptions, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(_testPatients);

            await Assert.ThrowsAsync<ParquetDataProcessorException>(
                () => parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("InvalidSchemaType")));
        }

        [Fact]
        public static async Task GivenInvalidJsonBatchData_WhenProcess_ExceptionShouldBeThrown()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManagerWithoutCustomizedSchema, _arrowConfigurationOptions, _defaultConverter, _fhirConverter, _nullParquetDataProcessorLogger);

            var invalidTestData = new JObject
            {
                { "resourceType", "Patient" },
                { "name", "Invalid field content, should be an array." },
            };

            var invalidJsonBatchData = new JsonBatchData(new List<JObject> { invalidTestData, invalidTestData });

            await Assert.ThrowsAsync<ParquetDataProcessorException>(() => parquetDataProcessor.ProcessAsync(invalidJsonBatchData, new ProcessParameters("Patient")));
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
