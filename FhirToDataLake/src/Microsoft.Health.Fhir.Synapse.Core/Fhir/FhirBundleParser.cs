﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir
{
    public static class FhirBundleParser
    {
        public static IEnumerable<JObject> ExtractResourcesFromBundle(JObject bundle)
        {
            var entries = bundle?.GetValue(FhirBundleConstants.EntryKey) as JArray;
            var resources = entries?
                .Select(entry => (entry as JObject)?.GetValue(FhirBundleConstants.EntryResourceKey) as JObject)
                .ToList();
            return resources ?? new List<JObject>();
        }

        public static IEnumerable<JObject> GetOperationOutcomes(IEnumerable<JObject> resources)
        {
            var operationOutcomes = resources.Where(x =>
                x?.GetValue(FhirBundleConstants.ResourceTypeKey)?.ToString() ==
                FhirBundleConstants.OperationOutcomeKey).ToList();
            return operationOutcomes ?? new List<JObject>();
        }

        public static string ExtractContinuationToken(JObject bundle)
        {
            var links = bundle?.GetValue(FhirBundleConstants.LinkKey) as JArray;
            if (links != null)
            {
                foreach (var link in links)
                {
                    var linkRelation = (link as JObject)?.GetValue(FhirBundleConstants.LinkRelationKey)?.Value<string>();
                    if (string.Equals(linkRelation, FhirBundleConstants.NextLinkValue, StringComparison.OrdinalIgnoreCase))
                    {
                        var nextLink = (link as JObject)?.GetValue(FhirBundleConstants.LinkUrlKey)?.Value<string>();
                        return ParseContinuationToken(nextLink);
                    }
                }
            }

            return null;
        }

        public static int ExtractVersionId(JObject bundle)
        {
            var meta = bundle?.GetValue(FhirBundleConstants.MetaKey) as JObject;
            var versionId = meta?.GetValue(FhirBundleConstants.VersionIdKey)?.Value<int>();
            return versionId ?? 0;
        }

        private static string ParseContinuationToken(string nextUrl)
        {
            if (string.IsNullOrEmpty(nextUrl))
            {
                return nextUrl;
            }

            var parameters = HttpUtility.ParseQueryString(nextUrl);
            return parameters.Get(FhirApiConstants.ContinuationKey);
        }
    }
}
