// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Health.AnalyticsConnector.Core.Fhir;
using Microsoft.Health.AnalyticsConnector.DataClient.UnitTests;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.Fhir
{
    public class FhirBundleParserTests
    {
        [Fact]
        public void GivenAnEmptyBundle_WhenParsingBundle_EmptyResultShouldBeReturned()
        {
            JObject bundle = TestDataProvider.GetBundleJsonFromFile(TestDataConstants.EmptyBundleFile);

            IEnumerable<JObject> resources = FhirBundleParser.ExtractResourcesFromBundle(bundle);
            string continuationToken = FhirBundleParser.ExtractContinuationToken(bundle);
            string resoruceId = FhirBundleParser.ExtractResourceId(bundle);

            Assert.Empty(resources);
            Assert.Null(continuationToken);
            Assert.Equal("bundle-example", resoruceId);
        }

        [Fact]
        public void GivenANullBundle_WhenParsingBundle_EmptyResultShouldBeReturned()
        {
            JObject bundle = null;

            IEnumerable<JObject> resources = FhirBundleParser.ExtractResourcesFromBundle(bundle);
            string continuationToken = FhirBundleParser.ExtractContinuationToken(bundle);
            string resoruceId = FhirBundleParser.ExtractResourceId(bundle);

            Assert.Empty(resources);
            Assert.Null(continuationToken);
            Assert.Null(resoruceId);
        }

        [Fact]
        public void GivenAValidBundle_WhenParsingBundle_CorrectResultShouldBeReturned()
        {
            JObject bundle = TestDataProvider.GetBundleJsonFromFile(TestDataConstants.BundleFile1);

            IEnumerable<JObject> resources = FhirBundleParser.ExtractResourcesFromBundle(bundle);
            string continuationToken = FhirBundleParser.ExtractContinuationToken(bundle);
            string resoruceId = FhirBundleParser.ExtractResourceId(resources.First());

            Assert.Equal(2, resources.Count());
            Assert.Equal("Y29udGludWF0aW9udG9rZW4=", continuationToken);
            Assert.Equal("3123", resoruceId);
        }

        [Fact]
        public void GivenValidOperationOutcomeResources_WhenGetOperationOutcomes_EmptyResultShouldBeReturned()
        {
            JObject jObj1 = JObject.Parse("{\"resourceType\":\"OperationOutcome\"}");
            JObject jObj2 = JObject.Parse(TestDataProvider.GetDataFromFile(TestDataConstants.InvalidResponseFile));
            List<JObject> input = new List<JObject> { jObj1, jObj2 };
            IEnumerable<JObject> results = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Equal(2, results.Count());

            input.Add(JObject.Parse("{\"resourceType\":\"Patient\"}"));
            IEnumerable<JObject> updatedResults = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Equal(2, updatedResults.Count());

            Assert.Equal(results.ToString(), updatedResults.ToString());
        }

        [Fact]
        public void GivenEmptyResources_WhenGetOperationOutcomes_EmptyResultShouldBeReturned()
        {
            List<JObject> input = new List<JObject>();
            IEnumerable<JObject> results = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Empty(results);

            input.Add(null);
            results = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Empty(results);
        }

        [Fact]
        public void GivenNullResources_WhenGetOperationOutcomes_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(() => FhirBundleParser.GetOperationOutcomes(null));
        }

        [Fact]
        public void GivenInvalidOperationOutcomeResources_WhenGetOperationOutcomes_EmptyResultShouldBeReturned()
        {
            JObject jObj1 = JObject.Parse("{\"a\":1}");
            JObject jObj2 = JObject.Parse("{\"resourceType\":\"Patient\"}");
            JObject jObj3 = JObject.Parse("{\"resourceType\":\"operationOutcome\"}");

            List<JObject> input = new List<JObject> { jObj1, jObj2, jObj3 };
            IEnumerable<JObject> results = FhirBundleParser.GetOperationOutcomes(input);
            Assert.Empty(results);
        }

        [Fact]
        public void GivenAValidBundle_WithNoNextLink_WhenParsingBatchData_CorrectResultShouldBeReturned()
        {
            JObject bundle = TestDataProvider.GetBundleJsonFromFile(TestDataConstants.BundleFile2);
            IEnumerable<JObject> resources = FhirBundleParser.ExtractResourcesFromBundle(bundle);
            string continuationToken = FhirBundleParser.ExtractContinuationToken(bundle);

            Assert.Single(resources);
            Assert.Null(continuationToken);
        }
    }
}
