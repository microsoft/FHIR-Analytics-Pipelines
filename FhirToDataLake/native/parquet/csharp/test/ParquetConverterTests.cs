// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Xunit;

namespace Microsoft.Health.Parquet.UnitTests
{
    public class ParquetConverterTests
    {
        private const string PatientResourceType = "Patient";
        private static string _testPatientSmallData;
        private static string _testPatientNormalData;
        private static string _testPatientSchema;
        private static string _testPatientSimplestSchema;

        public ParquetConverterTests()
        {
            _testPatientSmallData = File.ReadAllText(TestConstants.InputPatientSmallFile);
            _testPatientNormalData = File.ReadAllText(TestConstants.InputPatientNormalFile);
            _testPatientSchema = File.ReadAllText(TestConstants.ExampleSchemaFile);
            _testPatientSimplestSchema = File.ReadAllText(TestConstants.ExampleSimplestSchemaFile);
        }

        public static IEnumerable<object[]> GetInvalidSchemaContents()
        {
            yield return new object[] { File.ReadAllText("./TestData/TestSchema/Invalid_schema_no_subnodes.json") };
            yield return new object[] { File.ReadAllText("./TestData/TestSchema/Invalid_schema_invalid_subnodes.json") };
            yield return new object[] { File.ReadAllText("./TestData/TestSchema/Invalid_schema_repeated_not_match1.json") };
            yield return new object[] { File.ReadAllText("./TestData/TestSchema/Invalid_schema_repeated_not_match2.json") };
            yield return new object[] { File.ReadAllText("./TestData/TestSchema/Invalid_schema_property_type_not_match1.json") };
            yield return new object[] { File.ReadAllText("./TestData/TestSchema/Invalid_schema_property_type_not_match2.json") };
            yield return new object[] { File.ReadAllText("./TestData/TestSchema/Invalid_schema_property_type_not_match3.json") };
            yield return new object[] { File.ReadAllText("./TestData/TestSchema/Invalid_schema_property_type_not_match4.json") };
        }

        [Theory]
        [InlineData("Invalid Json")]
        [InlineData("")]
        [InlineData(" ")]
        [InlineData(null)]
        public void GivenInvalidSchema_WhenInitializeSchemaSet_ExceptionShouldBeThrown(string invalidSchema)
        {
            var invalidSchemaMap = new Dictionary<string, string> { { PatientResourceType, invalidSchema } };
            var exception = Assert.Throws<ParquetException>(() => ParquetConverter.CreateWithSchemaSet(invalidSchemaMap));
            Assert.Equal("Parse given schema failed.", exception.Message);
        }

        [Theory]
        [InlineData("")]
        [InlineData(" ")]
        public void GivenInvalidSchemaKey_WhenInitializeSchemaSet_ExceptionShouldBeThrown(string invalidSchemaKey)
        {
            var invalidSchemaMap = new Dictionary<string, string> { { invalidSchemaKey, _testPatientSchema } };
            var exception = Assert.Throws<ParquetException>(() => ParquetConverter.CreateWithSchemaSet(invalidSchemaMap));
            Assert.Equal("Parse given schema failed.", exception.Message);
        }

        [Fact]
        public void GivenNoSchemaFile_WhenConvertingToParquet_ExceptionShouldBeThrown()
        {
            var parquetConverter = new ParquetConverter();
            var exception = Assert.Throws<ParquetException>(() => parquetConverter.ConvertJsonToParquet(PatientResourceType, _testPatientSmallData));
            Assert.StartsWith("Target schema is not found.", exception.Message);
        }

        [Theory]
        [InlineData("")]
        [InlineData("123456")]
        [InlineData("#@!asdasd(*&^")]
        [InlineData(null)]
        public void GivenInvalidPatient_WhenConvertingToParquet_ExceptionShouldBeThrown(string patient)
        {
            var validSchemaMap = new Dictionary<string, string> { { PatientResourceType, _testPatientSchema } };
            var parquetConverter = ParquetConverter.CreateWithSchemaSet(validSchemaMap);

            var exception = Assert.Throws<ParquetException>(() => parquetConverter.ConvertJsonToParquet(PatientResourceType, patient));
            Assert.StartsWith("Input json is invalid.", exception.Message);
        }

        [Fact]
        public void GivenValidPatient_WhenConvertingToParquet_ResultShouldBeReturned()
        {
            var validSchemaMap = new Dictionary<string, string> { { PatientResourceType, _testPatientSchema } };
            var parquetConverter = ParquetConverter.CreateWithSchemaSet(validSchemaMap);

            using var stream = parquetConverter.ConvertJsonToParquet(PatientResourceType, _testPatientSmallData);
            var expectedHash = GetFileHash(TestConstants.ExpectedPatientSmallParquetFile);
            var streamHash = GetStreamHash(stream);
            Assert.Equal(expectedHash, streamHash);
        }

        [Fact]
        public void GivenValidPatient_WhenConvertingToParquet_WithSimplestSchema_ResultShouldBeReturned()
        {
            var validSchemaMap = new Dictionary<string, string> { { PatientResourceType, _testPatientSimplestSchema } };
            var parquetConverter = ParquetConverter.CreateWithSchemaSet(validSchemaMap);

            using var stream = parquetConverter.ConvertJsonToParquet(PatientResourceType, _testPatientSmallData);
            var expectedHash = GetFileHash(TestConstants.ExpectedPatientSmallParquetFile);
            var streamHash = GetStreamHash(stream);
            Assert.Equal(expectedHash, streamHash);
        }

        [Theory]
        [MemberData(nameof(GetInvalidSchemaContents))]
        public void GivenInvalidSchemaContents_WhenConvertingToParquet_ExceptionShouldBeThrown(string invalidSchema)
        {
            var validSchemaMap = new Dictionary<string, string> { { PatientResourceType, invalidSchema } };
            var parquetConverter = ParquetConverter.CreateWithSchemaSet(validSchemaMap);

            Assert.Throws<ParquetException>(() => parquetConverter.ConvertJsonToParquet(PatientResourceType, _testPatientSmallData));
        }

        [Theory]
        [InlineData(20)]
        [InlineData(200)]
        public void GivenValidPatient_WhenConvertingToParquet_WithMultipleTimes_ResultShouldBeReturned(int multipleTimes)
        {
            var validSchemaMap = new Dictionary<string, string> { { PatientResourceType, _testPatientSchema } };
            var parquetConverter = ParquetConverter.CreateWithSchemaSet(validSchemaMap);

            Stream stream = new MemoryStream();
            for (int i = 0; i < multipleTimes; i++)
            {
                stream = parquetConverter.ConvertJsonToParquet(PatientResourceType, _testPatientNormalData);
            }

            var expectedHash = GetFileHash(string.Format(TestConstants.ExpectedPatientNormalParquetFile, 1));
            var streamHash = GetStreamHash(stream);
            Assert.Equal(expectedHash, streamHash);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(20)]
        public void GivenValidLargePatient_WhenConvertingToParquet_ResultShouldBeReturned(int multipleCount)
        {
            var validSchemaMap = new Dictionary<string, string> { { PatientResourceType, _testPatientSchema } };
            var parquetConverter = ParquetConverter.CreateWithSchemaSet(validSchemaMap);

            var largePatientBuilder = new StringBuilder();
            for (int i = 0; i < multipleCount; i++)
            {
                largePatientBuilder.Append(_testPatientNormalData);
            }

            using var stream = parquetConverter.ConvertJsonToParquet(PatientResourceType, largePatientBuilder.ToString());

            var expectedHash = GetFileHash(string.Format(TestConstants.ExpectedPatientNormalParquetFile, multipleCount));
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
