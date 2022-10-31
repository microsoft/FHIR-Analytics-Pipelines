// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Hl7.Fhir.ElementModel;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Extensions
{
    public class ITypedElementExtensionsTests
    {
        private const string SamplePatientFileName = "TestData/PatientSample.json";
        private const string NoMetaSamplePatientFileName = "TestData/NoMetaPatientSample.json";

        public static IEnumerable<object[]> GetPropertyValuePairs()
        {
            return new List<object[]>
            {
                new object[] { "id", "81" },
                new object[] { "birthDate", "1932-03-23" },
                new object[] { "gender", "female" },
            };
        }

        [Fact]
        public void GivenATypedElement_WhenGetAPropertyNotExists_NullValueShouldReturn()
        {
            ITypedElement testElement = GetTestElement(SamplePatientFileName);

            string value = testElement.GetPropertyValue("abc")?.ToString();

            Assert.Null(value);
        }

        [Theory]
        [MemberData(nameof(GetPropertyValuePairs))]
        public void GivenATypedElement_WhenGetPropertyValue_CorrectValueShouldReturn(string propertyName, string expectedValue)
        {
            ITypedElement testElement = GetTestElement(SamplePatientFileName);

            string value = testElement.GetPropertyValue(propertyName).ToString();

            Assert.Equal(expectedValue, value);
        }

        [Fact]
        public void GivenATypedElement_WhenGetLastUpdatedDay_CorrectResultShouldReturn()
        {
            ITypedElement testElement = GetTestElement(SamplePatientFileName);

            DateTime? date = testElement.GetLastUpdatedDay();

            Assert.Equal(2012, date.Value.Year);
            Assert.Equal(6, date.Value.Month);
            Assert.Equal(3, date.Value.Day);
        }

        [Fact]
        public void GivenATypedElementWithoutLastUpdatedDay_WhenGetLastUpdatedDay_NullShouldReturn()
        {
            ITypedElement testElement = GetTestElement(NoMetaSamplePatientFileName);

            DateTime? date = testElement.GetLastUpdatedDay();

            Assert.Null(date);
        }

        [Fact]
        public void GivenAnEmptyElementDataList_WhenConvertToJObjects_EmptyBatchDataShouldReturn()
        {
            List<ITypedElement> elements = new List<ITypedElement>();
            JsonBatchData jsonBatchData = elements.ToJsonBatchData();
            Assert.Empty(jsonBatchData.Values);

            IEnumerable<ITypedElement> nullElements = null;
            JsonBatchData jsonBatchData2 = nullElements.ToJsonBatchData();
            Assert.Null(jsonBatchData2);
        }

        private ITypedElement GetTestElement(string fileName)
        {
            FhirServerConfiguration fhirConfiguration = new FhirServerConfiguration()
            {
                Version = FhirVersion.R4,
            };
            FhirSerializer fhirParser = new FhirSerializer(Options.Create(fhirConfiguration));
            return fhirParser.DeserializeToElement(File.ReadAllText(fileName));
        }
    }
}
