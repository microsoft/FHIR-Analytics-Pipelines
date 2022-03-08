// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.IO;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Extensions;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Extensions
{
    public class JObjectExtensionsTest
    {
        private const string SamplePatientFileName = "TestData/PatientSample.json";
        private const string NoMetaSamplePatientFileName = "TestData/NoMetaPatientSample.json";

        [Fact]
        public void GivenAJObject_WhenGetLastUpdatedDay_CorrectResultShouldReturn()
        {
            var testJObject = GetTestJObject(SamplePatientFileName);

            var date = testJObject.GetLastUpdatedDay();

            Assert.Equal(2012, date.Value.Year);
            Assert.Equal(6, date.Value.Month);
            Assert.Equal(3, date.Value.Day);
        }

        [Fact]
        public void GivenAJObjectWithoutLastUpdatedDay_WhenGetLastUpdatedDay_ExceptionShouldBeThrown()
        {
            var testJObject = GetTestJObject(NoMetaSamplePatientFileName);

            Assert.Throws<FhirDataParseExeption>(() => testJObject.GetLastUpdatedDay());
        }

        private JObject GetTestJObject(string fileName)
        {
            return JObject.Parse(File.ReadAllText(fileName));
        }
    }
}
