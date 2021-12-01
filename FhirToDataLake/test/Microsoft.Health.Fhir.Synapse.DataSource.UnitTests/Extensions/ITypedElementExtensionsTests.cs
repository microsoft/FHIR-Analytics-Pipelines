// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Hl7.Fhir.ElementModel;
using Microsoft.Health.Fhir.Synapse.DataSource.Extensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataSource.UnitTests.Extensions
{
    public class ITypedElementExtensionsTests
    {
        public static IEnumerable<object[]> ElementsData =>
            new List<object[]>
            {
                new object[] { null, "name", null },
                new object[]
                {
                    TestDataProvider.GetBundleElementFromFile(TestDataConstants.BundleFile1),
                    "type",
                    "searchset",
                },
                new object[]
                {
                    TestDataProvider.GetBundleElementFromFile(TestDataConstants.BundleFile1)
                        .Children("link").First(),
                    "relation",
                    "self",
                },
            };

        [Theory]
        [MemberData(nameof(ElementsData))]
        public void GivenATypedElement_WhenGetPropertyName_CorrectValueShouldBeReturned(
            ITypedElement element,
            string propertyName,
            string expectedValue)
        {
            var propertyValue = element.GetPropertyValue(propertyName);
            Assert.Equal(expectedValue, propertyValue);
        }
    }
}
