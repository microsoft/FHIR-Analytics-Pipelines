// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.Core.Extensions;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.Extensions
{
    public class JObjectExtensionsTest
    {
        private const string SamplePatientFileName = "TestData/PatientSample.json";
        private const string NoMetaSamplePatientFileName = "TestData/NoMetaPatientSample.json";

        [Fact]
        public void GivenAJObject_WhenGetLastUpdated_CorrectResultShouldReturn()
        {
            JObject testJObject = GetTestJObject(SamplePatientFileName);

            DateTimeOffset? date = testJObject.GetLastUpdated();
            Assert.NotNull(date);

            Assert.Equal(2012, date.Value.Year);
            Assert.Equal(6, date.Value.Month);
            Assert.Equal(3, date.Value.Day);
        }

        [Fact]
        public void GivenAJObjectWithoutLastUpdated_WhenGetLastUpdated_ExceptionShouldBeThrown()
        {
            JObject testJObject = GetTestJObject(NoMetaSamplePatientFileName);

            Assert.Throws<FhirDataParseException>(() => testJObject.GetLastUpdated());
        }

        private JObject GetTestJObject(string fileName)
        {
            return JObject.Parse(File.ReadAllText(fileName));
        }
    }
}
