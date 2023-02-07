// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Web;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Fhir;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.AnalyticsConnector.Core.Fhir
{
    public static class FhirBundleParser
    {
        public static IEnumerable<JObject> ExtractResourcesFromBundle(JObject bundle)
        {
            var entries = bundle?.GetValue(FhirBundleConstants.EntryKey) as JArray;
            List<JObject> resources = entries?
                .Select(entry => (entry as JObject)?.GetValue(FhirBundleConstants.EntryResourceKey) as JObject)
                .ToList();
            return resources ?? new List<JObject>();
        }

        public static IEnumerable<JObject> GetOperationOutcomes(IEnumerable<JObject> resources)
        {
            List<JObject> operationOutcomes = resources.Where(x =>
                x?.GetValue(FhirBundleConstants.ResourceTypeKey)?.ToString() ==
                FhirConstants.OperationOutcomeResource).ToList();
            return operationOutcomes ?? new List<JObject>();
        }

        public static string ExtractContinuationToken(JObject bundle)
        {
            var links = bundle?.GetValue(FhirBundleConstants.LinkKey) as JArray;
            if (links != null)
            {
                foreach (JToken link in links)
                {
                    string linkRelation = (link as JObject)?.GetValue(FhirBundleConstants.LinkRelationKey)?.Value<string>();
                    if (string.Equals(linkRelation, FhirBundleConstants.NextLinkValue, StringComparison.OrdinalIgnoreCase))
                    {
                        string nextLink = (link as JObject)?.GetValue(FhirBundleConstants.LinkUrlKey)?.Value<string>();
                        return ParseContinuationToken(nextLink);
                    }
                }
            }

            return null;
        }

        public static int ExtractVersionId(JObject bundle)
        {
            var meta = bundle?.GetValue(FhirBundleConstants.MetaKey) as JObject;
            int? versionId = meta?.GetValue(FhirBundleConstants.VersionIdKey)?.Value<int>();
            return versionId ?? 0;
        }

        private static string ParseContinuationToken(string nextUrl)
        {
            if (string.IsNullOrEmpty(nextUrl))
            {
                return nextUrl;
            }

            NameValueCollection parameters = HttpUtility.ParseQueryString(nextUrl);
            return parameters.Get(FhirApiConstants.ContinuationKey);
        }
    }
}
