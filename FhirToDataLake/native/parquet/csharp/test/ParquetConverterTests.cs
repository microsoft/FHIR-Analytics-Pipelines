// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Health.Parquet;
using Xunit;

namespace Microsoft.Health.Parquet.UnitTests
{
    public class ParquetConverterTests
    {
        private const string PatientResourceType = "Patient";
        private const string InputPatientFile = "./TestData/Patient.ndjson";
        private const string SchemaFile = "./TestData/patient_example_schema.json";
        private const string ExpectedPatientParquetFile = "./TestData/Expected/expected_patient.parquet";

        [Fact]
        public void GivenInvalidSchemaFile_WhenInitializeSchema_ExceptionShouldBeThrown()
        {
            var invalidSchemaMap = new Dictionary<string, string> { { PatientResourceType, "Invalid json" } };
            var exception = Assert.Throws<ParquetException>(() => ParquetConverter.CreateWithSchemaSet(invalidSchemaMap));
            Assert.Equal("Parse given schema failed.", exception.Message);
        }

        [Fact]
        public void GivenNoSchemaFile_WhenConvertingToJson_ExceptionShouldBeThrown()
        {
            var parquetConverter = new ParquetConverter();
            string jsonInput = File.ReadAllText(InputPatientFile);
            var exception = Assert.Throws<ParquetException>(() => parquetConverter.ConvertJsonToParquet(PatientResourceType, jsonInput));
            Assert.StartsWith("Target schema is not found.", exception.Message);
        }

        [Theory]
        [InlineData("")]
        [InlineData("123456")]
        [InlineData("#@!asdasd(*&^")]
        public void GivenInvalidPatient_WhenConvertingToJson_ExceptionShouldBeThrown(string patient)
        {
            var validSchemaMap = new Dictionary<string, string> { { PatientResourceType, File.ReadAllText(SchemaFile) } };
            var parquetConverter = ParquetConverter.CreateWithSchemaSet(validSchemaMap);

            var exception = Assert.Throws<ParquetException>(() => parquetConverter.ConvertJsonToParquet(PatientResourceType, patient));
            Assert.StartsWith("Input json is invalid.", exception.Message);
        }

        [Fact]
        public void GivenValidPatient_WhenConvertingToJson_ResultShouldBeReturned()
        {
            var validSchemaMap = new Dictionary<string, string> { { PatientResourceType, File.ReadAllText(SchemaFile) } };
            var parquetConverter = ParquetConverter.CreateWithSchemaSet(validSchemaMap);

            using var stream = parquetConverter.ConvertJsonToParquet(PatientResourceType, File.ReadAllText(InputPatientFile));
            var expectedHash = GetFileHash(ExpectedPatientParquetFile);
            var streamHash = GetStreamHash(stream);
            Assert.Equal(expectedHash, streamHash);
        }

        private string GetFileHash(string filename)
        {
            using SHA256 hash = SHA256.Create();
            var clearBytes = File.ReadAllBytes(filename);
            var hashedBytes = hash.ComputeHash(clearBytes);
            return ConvertBytesToHex(hashedBytes);
        }

        private string GetStreamHash(Stream sourceStream)
        {
            using SHA256 hash = SHA256.Create();
            using var memoryStream = new MemoryStream();
            sourceStream.CopyTo(memoryStream);
            var bytes = memoryStream.ToArray();
            var hashedBytes = hash.ComputeHash(bytes);
            return ConvertBytesToHex(hashedBytes);
        }

        private string ConvertBytesToHex(byte[] bytes)
        {
            var sb = new StringBuilder();

            for (var i = 0; i < bytes.Length; i++)
            {
                sb.Append(bytes[i].ToString("x"));
            }

            return sb.ToString();
        }
    }
}
