﻿// -------------------------------------------------------------------------------------------------
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
using Microsoft.Health.Fhir.Synapse.DataSerialization.Parquet;
using Microsoft.Health.Fhir.Synapse.DataSerialization.Parquet.Exceptions;
using Microsoft.Health.Fhir.Synapse.Schema;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataSerialization.UnitTests.Parquet
{
    public class ParquetDataProcessorTests
    {
        private static readonly FhirSchemaManager _fhirSchemaManager;
        private static readonly IOptions<ArrowConfiguration> _arrowConfigurationOptions;
        private static readonly NullLogger<ParquetDataProcessor> _nullParquetDataProcessorLogger = NullLogger<ParquetDataProcessor>.Instance;

        private const string _testDataFolder = "./Parquet/TestData";
        private const string _expectTestDataFolder = "./Parquet/TestData/Expected";
        private static readonly List<JObject> _testPatientData;

        static ParquetDataProcessorTests()
        {
            var schemaConfigurationOption = Options.Create(new SchemaConfiguration()
            {
                SchemaCollectionDirectory = TestUtils.SchemaDirectoryPath,
            });

            _fhirSchemaManager = new FhirSchemaManager(schemaConfigurationOption, NullLogger<FhirSchemaManager>.Instance);
            _arrowConfigurationOptions = Options.Create(new ArrowConfiguration());

            _testPatientData = TestUtils.LoadNdjsonData(Path.Combine(_testDataFolder, "Basic_Patient.ndjson")).ToList();
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

            var jsonBatchData = new JsonBatchData(_testPatientData);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, TestUtils.GetTestTaskContext("Patient"));

            var resultStream = new MemoryStream();
            resultBatchData.Value.CopyTo(resultStream);

            var expectedResult = GetExpectedParquetStream(Path.Combine(_expectTestDataFolder, "Expected_Patient.parquet"));

            Assert.Equal(expectedResult.ToArray(), resultStream.ToArray());
        }

        // It may takes few minutes to run this large input data test.
        [Fact]
        public static async Task GivenAValidMultipleLargeInputData_WhenProcess_CorrectResultShouldBeReturned()
        {
            var largePatientSingleSet = TestUtils.LoadNdjsonData(Path.Combine(_testDataFolder, "Large_Patient.ndjson"));
            var largeTestData = Enumerable.Repeat(largePatientSingleSet, 100).SelectMany(x => x);

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(largeTestData);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, TestUtils.GetTestTaskContext("Patient"));

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

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, TestUtils.GetTestTaskContext("Patient"));

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

            var testData = new List<JObject>(_testPatientData) { shortPatientData };

            // Set BlockSize small here, only shortPatientData can be retained an be converting to parquet result.
            var arrowConfigurationOptions = Options.Create(new ArrowConfiguration()
            {
                ReadOptions = new ArrowReadOptionsConfiguration() { BlockSize = 50 },
            });

            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(testData);

            var resultBatchData = await parquetDataProcessor.ProcessAsync(jsonBatchData, TestUtils.GetTestTaskContext("Patient"));

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

            var testData = new List<JObject>(_testPatientData);
            var jsonBatchData = new JsonBatchData(testData);

            StreamBatchData result = await parquetDataProcessor.ProcessAsync(jsonBatchData, TestUtils.GetTestTaskContext("Patient"));
            Assert.Null(result);
        }

        [Fact]
        public static async Task GivenInvalidResourceType_WhenProcess_ExceptionShouldBeThrown()
        {
            var parquetDataProcessor = new ParquetDataProcessor(_fhirSchemaManager, _arrowConfigurationOptions, _nullParquetDataProcessorLogger);

            var jsonBatchData = new JsonBatchData(_testPatientData);

            await Assert.ThrowsAsync<ParquetDataProcessorException>(
                () => parquetDataProcessor.ProcessAsync(jsonBatchData, TestUtils.GetTestTaskContext("InvalidResourceType")));
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

            await Assert.ThrowsAsync<ParquetDataProcessorException>(() => parquetDataProcessor.ProcessAsync(invalidJsonBatchData, TestUtils.GetTestTaskContext("Patient")));
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
