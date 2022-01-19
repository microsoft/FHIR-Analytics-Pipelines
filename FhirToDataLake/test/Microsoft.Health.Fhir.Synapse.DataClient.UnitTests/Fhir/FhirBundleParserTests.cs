// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using Hl7.Fhir.ElementModel;
using Microsoft.Health.Fhir.Synapse.DataClient.Fhir;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Fhir
{
    public class FhirBundleParserTests
    {
        [Fact]
        public void GivenAnEmptyBundle_WhenParsingBatchData_EmptyResultShouldBeReturned()
        {
            var bundleElement = TestDataProvider.GetBundleElementFromFile(TestDataConstants.EmptyBundleFile);

            var batchData = FhirBundleParser.ParseBatchData(bundleElement);

            Assert.Empty(batchData.Values);
            Assert.Null(batchData.ContinuationToken);
        }

        [Fact]
        public void GivenANullBundle_WhenParsingBatchData_ExceptionShouldBeThrown()
        {
            ITypedElement bundleElement = null;

            Assert.Throws<ArgumentNullException>(() => FhirBundleParser.ParseBatchData(bundleElement));
        }

        [Fact]
        public void GivenAValidBundle_WhenParsingBatchData_CorrectResultShouldBeReturned()
        {
            var bundleElement = TestDataProvider.GetBundleElementFromFile(TestDataConstants.BundleFile1);

            var batchData = FhirBundleParser.ParseBatchData(bundleElement);

            Assert.Equal(2, batchData.Values.Count());
            Assert.Equal("Y29udGludWF0aW9udG9rZW4=", batchData.ContinuationToken);
        }

        [Fact]
        public void GivenAValidBundle_WithNoNextLink_WhenParsingBatchData_CorrectResultShouldBeReturned()
        {
            var bundleElement = TestDataProvider.GetBundleElementFromFile(TestDataConstants.BundleFile2);

            var batchData = FhirBundleParser.ParseBatchData(bundleElement);

            Assert.Single(batchData.Values);
            Assert.Null(batchData.ContinuationToken);
        }
    }
}
