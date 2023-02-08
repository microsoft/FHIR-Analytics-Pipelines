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
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Models.Data;
using Microsoft.Health.AnalyticsConnector.Core.DataProcessor.DataConverter;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.Parquet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.DataProcessor.DataConverter
{
    public class FhirDefaultSchemaConverterTests
    {
        private static readonly IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private static readonly JObject _testPatient;
        private static readonly FhirDefaultSchemaConverter _testDefaultConverter;

        static FhirDefaultSchemaConverterTests()
        {
            var schemaManager = new ParquetSchemaManager(
                Options.Create(new SchemaConfiguration()),
                TestUtils.TestFhirParquetSchemaProviderDelegate,
                _diagnosticLogger,
                NullLogger<ParquetSchemaManager>.Instance);

            _testDefaultConverter = new FhirDefaultSchemaConverter(schemaManager, _diagnosticLogger, NullLogger<FhirDefaultSchemaConverter>.Instance);
            _testPatient = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.TestDataFolder, "Basic_Raw_Patient.ndjson")).First();
        }

        public static IEnumerable<object[]> GetInvalidSchemaContents()
        {
            yield return new object[] { File.ReadAllText(Path.Join(TestUtils.TestInvalidSchemaDirectoryPath, "Invalid_schema_repeated_not_match1.json")) };
            yield return new object[] { File.ReadAllText(Path.Join(TestUtils.TestInvalidSchemaDirectoryPath, "Invalid_schema_repeated_not_match2.json")) };
        }

        public static IEnumerable<object[]> GetValidDataContents()
        {
            yield return new object[]
            {
                "Patient",

                // Struct data.
                new JObject
                {
                    {
                        "text", new JObject
                        {
                            { "status", "generated" },
                            { "div", "Test div in text" },
                        }
                    },
                },

                // Expected struct format fields are same with raw struct format fields.
                new JObject
                {
                    {
                        "text", new JObject
                        {
                            { "status", "generated" },
                            { "div", "Test div in text" },
                        }
                    },
                },
            };

            yield return new object[]
            {
                "Patient",

                // Array data.
                new JObject
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
                },

                // Expected array format fields are same with raw array format fields.
                new JObject
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
                },
            };

            yield return new object[]
            {
                "Patient",

                // Data with deep array field
                new JObject
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
                },

                new JObject
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
                },
            };

            yield return new object[]
            {
                "Patient",

                // Data with deep fields
                new JObject
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
                },

                new JObject
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
                },
            };

            yield return new object[]
            {
                "Observation",

                // Data with primitive choice type
                new JObject
                {
                    { "effectiveDateTime", "1905-08-23" },
                },

                // Primitive choice data type
                new JObject
                {
                    { "effective", new JObject { { "dateTime", "1905-08-23" } } },
                },
            };

            yield return new object[]
            {
                "Observation",

                // Data with struct choice type
                new JObject
                {
                    { "effectivePeriod", new JObject { { "start", "1905-08-23" } } },
                },

                // Struct choice data type
                new JObject
                {
                    { "effective", new JObject { { "period", new JObject { { "start", "1905-08-23" } } } } },
                },
            };
        }

        public static IEnumerable<object[]> GetInvalidDataContents()
        {
            yield return new object[]
            {
                "Patient",
                new JObject
                {
                    { "name", "Invalid data fields, should be array." },
                },
            };

            yield return new object[]
            {
                "Patient",
                new JObject
                {
                    {
                        "text", null
                    },
                },
            };

            yield return new object[]
            {
                "Patient",
                new JObject
                {
                    {
                        "name", new JArray { null }
                    },
                },
            };

            yield return new object[]
            {
                "Patient",
                null,
            };
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            var schemaManager = new ParquetSchemaManager(
                Options.Create(new SchemaConfiguration()),
                TestUtils.TestFhirParquetSchemaProviderDelegate,
                _diagnosticLogger,
                NullLogger<ParquetSchemaManager>.Instance);

            Assert.Throws<ArgumentNullException>(
                () => new FhirDefaultSchemaConverter(null, _diagnosticLogger, NullLogger<FhirDefaultSchemaConverter>.Instance));

            Assert.Throws<ArgumentNullException>(
                () => new FhirDefaultSchemaConverter(schemaManager, null, NullLogger<FhirDefaultSchemaConverter>.Instance));

            Assert.Throws<ArgumentNullException>(
                () => new FhirDefaultSchemaConverter(schemaManager, _diagnosticLogger, null));
        }

        [Fact]
        public void GivenAValidBasicSchema_WhenConvert_CorrectResultShouldBeReturned()
        {
            var result = _testDefaultConverter.Convert(
                CreateTestJsonBatchData(_testPatient),
                "Patient");

            var expectedResult = TestUtils.LoadNdjsonData(Path.Combine(TestUtils.ExpectTestDataFolder, "Expected_Processed_Patient.ndjson"));

            Assert.True(JToken.DeepEquals(result.Values.First(), expectedResult.First()));
        }

        [Theory]
        [MemberData(nameof(GetValidDataContents))]
        public void GivenAValidData_WhenConvert_CorrectResultShouldBeReturned(string schemaType, JObject inputObject, JObject expectedObject)
        {
            JsonBatchData result = _testDefaultConverter.Convert(CreateTestJsonBatchData(inputObject), schemaType);
            Assert.True(JToken.DeepEquals(result.Values.First(), expectedObject));
        }

        [Fact]
        public void GivenNullSchema_WhenConvert_ExceptionShouldBeReturned()
        {
            Assert.Throws<ArgumentNullException>(
                () => _testDefaultConverter.Convert(CreateTestJsonBatchData(_testPatient), null));

            Assert.Throws<ArgumentNullException>(
                () => _testDefaultConverter.Convert(null, "Observation"));
        }

        [Fact]
        public void GivenNoExistSchema_WhenConvert_ExceptionShouldBeReturned()
        {
            Assert.Throws<ParquetDataProcessorException>(
                () => _testDefaultConverter.Convert(CreateTestJsonBatchData(_testPatient), "No_Exist_Schema"));
        }

        [Theory]
        [MemberData(nameof(GetInvalidSchemaContents))]
        public void GivenInvalidSchema_WhenConvert_ExceptionShouldBeThrown(string invalidSchemaContent)
        {
            var schemaNode = JsonConvert.DeserializeObject<ParquetSchemaNode>(invalidSchemaContent);
            var schemaManager = CreateMockSchemaManager(schemaNode);
            var testConverter = new FhirDefaultSchemaConverter(schemaManager, _diagnosticLogger, NullLogger<FhirDefaultSchemaConverter>.Instance);

            Assert.Throws<ParquetDataProcessorException>(()
                => testConverter.Convert(CreateTestJsonBatchData(_testPatient), "Patient").Values.Count());
        }

        [Theory]
        [MemberData(nameof(GetInvalidDataContents))]
        public void GivenInvalidData_WhenConvert_ExceptionShouldBeThrown(string schemaType, JObject inputObject)
        {
            Assert.Throws<ParquetDataProcessorException>(()
                => _testDefaultConverter.Convert(CreateTestJsonBatchData(inputObject), schemaType).Values.Count());
        }

        private static JsonBatchData CreateTestJsonBatchData(JObject testJObjectData)
        {
            List<JObject> testData = new List<JObject>() { testJObjectData };
            return new JsonBatchData(testData);
        }

        private static ISchemaManager<ParquetSchemaNode> CreateMockSchemaManager(ParquetSchemaNode schemaNode)
        {
            var mockSchemaManager = Substitute.For<ISchemaManager<ParquetSchemaNode>>();
            mockSchemaManager.GetSchema(Arg.Any<string>()).Returns(schemaNode);

            return mockSchemaManager;
        }
    }
}
