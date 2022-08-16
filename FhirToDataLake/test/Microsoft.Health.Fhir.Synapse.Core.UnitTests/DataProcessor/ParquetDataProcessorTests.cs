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
using Microsoft.Health.Fhir.Synapse.SchemaManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.DataProcessor
{
    public class ParquetDataProcessorTests
    {
        private static readonly IFhirSchemaManager<FhirParquetSchemaNode> _testDefaultFhirSchemaManager;
        private static readonly ParquetDataProcessor _testParquetDataProcessorWithoutCustomizedSchema;
        private static readonly ParquetDataProcessor _testParquetDataProcessorWithCustomizedSchema;

        private static readonly List<JObject> _testPatients;
        private static readonly JObject _testPatient;

        static ParquetDataProcessorTests()
        {
            var fhirSchemaManagerWithoutCustomizedSchema = new FhirParquetSchemaManager(
                Options.Create(new SchemaConfiguration()),
                TestUtils.TestParquetSchemaProviderDelegate,
                NullLogger<FhirParquetSchemaManager>.Instance);

            var fhirSchemaManagerWithCustomizedSchema = new FhirParquetSchemaManager(
                Options.Create(TestUtils.TestCustomSchemaConfiguration),
                TestUtils.TestParquetSchemaProviderDelegate,
                NullLogger<FhirParquetSchemaManager>.Instance);

            var arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            _testDefaultFhirSchemaManager = fhirSchemaManagerWithoutCustomizedSchema;

            _testParquetDataProcessorWithoutCustomizedSchema = new ParquetDataProcessor(
                fhirSchemaManagerWithoutCustomizedSchema,
                arrowConfigurationOptions,
                TestUtils.TestDataSchemaConverterDelegate,
                NullLogger<ParquetDataProcessor>.Instance);

            _testParquetDataProcessorWithCustomizedSchema = new ParquetDataProcessor(
                fhirSchemaManagerWithCustomizedSchema,
                arrowConfigurationOptions,
                TestUtils.TestDataSchemaConverterDelegate,
                NullLogger<ParquetDataProcessor>.Instance);

            _testPatient = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.TestDataFolder, "Basic_Raw_Patient.ndjson")).First();
            _testPatients = new List<JObject> { _testPatient, _testPatient };
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            var fhirSchemaManager = new FhirParquetSchemaManager(
                Options.Create(new SchemaConfiguration()),
                TestUtils.TestParquetSchemaProviderDelegate,
                NullLogger<FhirParquetSchemaManager>.Instance);

            var arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            var loggerInstance = NullLogger<ParquetDataProcessor>.Instance;

            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(null, arrowConfigurationOptions, TestUtils.TestDataSchemaConverterDelegate, loggerInstance));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(fhirSchemaManager, null, TestUtils.TestDataSchemaConverterDelegate, loggerInstance));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(fhirSchemaManager, arrowConfigurationOptions, null, loggerInstance));

            Assert.Throws<ArgumentNullException>(
                () => new ParquetDataProcessor(fhirSchemaManager, arrowConfigurationOptions, TestUtils.TestDataSchemaConverterDelegate, null));
        }

        [Fact]
        public async Task GivenAValidInputData_WhenProcessWithoutCustomizedSchema_CorrectResultShouldBeReturned()
        {
            var jsonBatchData = new JsonBatchData(_testPatients);

            var resultBatchData = await _testParquetDataProcessorWithoutCustomizedSchema.ProcessAsync(jsonBatchData, new ProcessParameters("Patient", "Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        [Fact]
        public async Task GivenAValidInputData_WhenProcessWithCustomizedSchema_CorrectResultShouldBeReturned()
        {
            var resultBatchData = await _testParquetDataProcessorWithCustomizedSchema.ProcessAsync(
                new JsonBatchData(_testPatients),
                new ProcessParameters($"Patient{FhirParquetSchemaConstants.CustomizedSchemaSuffix}", "Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient_Customized.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        // It may takes few minutes to run this large input data test.
        [Fact]
        public async Task GivenAValidMultipleLargeInputData_WhenProcess_CorrectResultShouldBeReturned()
        {
            var largePatientSingleSet = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.TestDataFolder, "Large_Patient.ndjson"));
            var largeTestData = Enumerable.Repeat(largePatientSingleSet, 50).SelectMany(x => x);

            var jsonBatchData = new JsonBatchData(largeTestData);

            var resultBatchData = await _testParquetDataProcessorWithoutCustomizedSchema.ProcessAsync(jsonBatchData, new ProcessParameters("Patient", "Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient_MultipleLargeSize.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        // It may takes few minutes to run this large input data test.
        [Fact]
        public async Task GivenAValidLargeInputData_WhenProcess_CorrectResultShouldBeReturned()
        {
            var largePatientSingleSet = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.TestDataFolder, "Large_Patient.ndjson"));

            var jsonBatchData = new JsonBatchData(largePatientSingleSet);

            var resultBatchData = await _testParquetDataProcessorWithoutCustomizedSchema.ProcessAsync(jsonBatchData, new ProcessParameters("Patient", "Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient_LargeSize.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        [Fact]
        public async Task GivenDataWithSomeRecordsLengthLargerThanBlockSize_WhenProcess_LargeRecordsShouldBeIgnored()
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

            var parquetDataProcessor = new ParquetDataProcessor(
                _testDefaultFhirSchemaManager,
                arrowConfigurationOptions,
                TestUtils.TestDataSchemaConverterDelegate,
                NullLogger<ParquetDataProcessor>.Instance);

            var jsonBatchData = new JsonBatchData(testData);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient", "Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Patient_IgnoreLargeLength.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        [Fact]
        public async Task GivenDataAllRecordsLengthLargerThanBlockSize_WhenProcess_NullResultShouldReturned()
        {
            // Set BlockSize small here, only shortPatientData can be retained an be converting to parquet result.
            var arrowConfigurationOptions = Options.Create(new ArrowConfiguration()
            {
                ReadOptions = new ArrowReadOptionsConfiguration() { BlockSize = 50 },
            });

            var parquetDataProcessor = new ParquetDataProcessor(
                _testDefaultFhirSchemaManager,
                arrowConfigurationOptions,
                TestUtils.TestDataSchemaConverterDelegate,
                NullLogger<ParquetDataProcessor>.Instance);

            var testData = new List<JObject>(_testPatients);
            var jsonBatchData = new JsonBatchData(testData);

            StreamBatchData result = await parquetDataProcessor.ProcessAsync(jsonBatchData, new ProcessParameters("Patient", "Patient"));
            Assert.Null(result);
        }

        [Fact]
        public async Task GivenInvalidSchemaOrResourceType_WhenProcess_ExceptionShouldBeThrown()
        {
            var jsonBatchData = new JsonBatchData(_testPatients);

            await Assert.ThrowsAsync<ParquetDataProcessorException>(
                () => _testParquetDataProcessorWithoutCustomizedSchema.ProcessAsync(jsonBatchData, new ProcessParameters("InvalidSchemaType", "InvalidSchemaType")));
        }

        [Fact]
        public async Task GivenInvalidJsonBatchData_WhenProcess_ExceptionShouldBeThrown()
        {
            var invalidTestData = new JObject
            {
                { "resourceType", "Patient" },
                { "name", "Invalid field content, should be an array." },
            };

            var invalidJsonBatchData = new JsonBatchData(new List<JObject> { invalidTestData, invalidTestData });

            await Assert.ThrowsAsync<ParquetDataProcessorException>(
                () => _testParquetDataProcessorWithoutCustomizedSchema.ProcessAsync(invalidJsonBatchData, new ProcessParameters("Patient", "Patient")));
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
