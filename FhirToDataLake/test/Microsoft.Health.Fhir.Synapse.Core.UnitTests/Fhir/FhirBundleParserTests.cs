// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Linq;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.DataClient.UnitTests;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Fhir
{
    public class FhirBundleParserTests
    {
        [Fact]
        public void GivenAnEmptyBundle_WhenParsingBundle_EmptyResultShouldBeReturned()
        {
            var bundle = TestDataProvider.GetBundleJsonFromFile(TestDataConstants.EmptyBundleFile);

            var resources = FhirBundleParser.ExtractResourcesFromBundle(bundle);
            var continuationToken = FhirBundleParser.ExtractContinuationToken(bundle);

            Assert.Empty(resources);
            Assert.Null(continuationToken);
        }

        [Fact]
        public void GivenANullBundle_WhenParsingBundle_EmptyResultShouldBeReturned()
        {
            JObject bundle = null;

            var resources = FhirBundleParser.ExtractResourcesFromBundle(bundle);
            var continuationToken = FhirBundleParser.ExtractContinuationToken(bundle);
            Assert.Empty(resources);
            Assert.Null(continuationToken);
        }

        [Fact]
        public void GivenAValidBundle_WhenParsingBundle_CorrectResultShouldBeReturned()
        {
            var bundle = TestDataProvider.GetBundleJsonFromFile(TestDataConstants.BundleFile1);

            var resources = FhirBundleParser.ExtractResourcesFromBundle(bundle);
            var continuationToken = FhirBundleParser.ExtractContinuationToken(bundle);

            Assert.Equal(2, resources.Count());
            Assert.Equal("Y29udGludWF0aW9udG9rZW4=", continuationToken);
        }

        [Fact]
        public void GivenAValidBundle_WithNoNextLink_WhenParsingBatchData_CorrectResultShouldBeReturned()
        {
            var bundle = TestDataProvider.GetBundleJsonFromFile(TestDataConstants.BundleFile2);
            var resources = FhirBundleParser.ExtractResourcesFromBundle(bundle);
            var continuationToken = FhirBundleParser.ExtractContinuationToken(bundle);

            Assert.Single(resources);
            Assert.Null(continuationToken);
        }
    }
}
