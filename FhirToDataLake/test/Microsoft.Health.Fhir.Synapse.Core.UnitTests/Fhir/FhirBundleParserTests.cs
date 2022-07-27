// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
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
        public void GivenValidOperationOutcomeResources_WhenGetOperationOutcomes_EmptyResultShouldBeReturned()
        {
            var jObj1 = JObject.Parse("{\"resourceType\":\"OperationOutcome\"}");
            var jObj2 = JObject.Parse(TestDataProvider.GetBundleFromFile(TestDataConstants.InvalidResponseFile));
            var input = new List<JObject> { jObj1, jObj2 };
            var results = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Equal(2, results.Count());

            input.Add(JObject.Parse("{\"resourceType\":\"Patient\"}"));
            var updatedResults = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Equal(2, updatedResults.Count());

            Assert.Equal(results.ToString(), updatedResults.ToString());
        }

        [Fact]
        public void GivenEmptyResources_WhenGetOperationOutcomes_EmptyResultShouldBeReturned()
        {
            var input = new List<JObject>();
            var results = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Empty(results);

            input.Add(null);
            results = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Empty(results);
        }

        [Fact]
        public void GivenNullResources_WhenGetOperationOutcomes_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>( () => FhirBundleParser.GetOperationOutcomes(null));
        }

        [Fact]
        public void GivenInvalidOperationOutcomeResources_WhenGetOperationOutcomes_EmptyResultShouldBeReturned()
        {
            var jObj1 = JObject.Parse("{\"a\":1}");
            var jObj2 = JObject.Parse("{\"resourceType\":\"Patient\"}");
            var jObj3 = JObject.Parse("{\"resourceType\":\"operationOutcome\"}");

            var input = new List<JObject>{jObj1, jObj2, jObj3};
            var results = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Empty(results);
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
