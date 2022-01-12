// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using EnsureThat;
using Hl7.Fhir.ElementModel;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Extensions;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Fhir
{
    public static class FhirBundleParser
    {
        /// <summary>
        /// Extract resource elements and continuation token from search bundle element.
        /// </summary>
        /// <param name="bundleElement">input bundle.</param>
        /// <returns>A FhirElementBatchData object.</returns>
        public static FhirElementBatchData ParseBatchData(ITypedElement bundleElement)
        {
            EnsureArg.IsNotNull(bundleElement, nameof(bundleElement));

            if (!string.Equals(bundleElement.InstanceType, FhirBundleConstants.BundleResourceType, StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("The input bundle element is invalid");
            }

            var elements = GetResourcesInBundle(bundleElement);
            var nextLink = GetNextLink(bundleElement);
            var continuationToken = ParseContinuationToken(nextLink);

            return new FhirElementBatchData(elements, continuationToken);
        }

        private static IEnumerable<ITypedElement> GetResourcesInBundle(ITypedElement bundleElement)
        {
            return bundleElement.Children(FhirBundleConstants.EntryKey)
                .SelectMany(entry => entry?.Children(FhirBundleConstants.EntryResourceKey));
        }

        private static string GetNextLink(ITypedElement bundleElement)
        {
            return bundleElement
                .Children(FhirBundleConstants.LinkKey)
                .FirstOrDefault(linkElement =>
                    string.Equals(
                        FhirBundleConstants.NextLinkValue,
                        linkElement.GetPropertyValue(FhirBundleConstants.LinkRelationKey)?.ToString(),
                        StringComparison.OrdinalIgnoreCase))
                .GetPropertyValue(FhirBundleConstants.LinkUrlKey)?
                .ToString();
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
