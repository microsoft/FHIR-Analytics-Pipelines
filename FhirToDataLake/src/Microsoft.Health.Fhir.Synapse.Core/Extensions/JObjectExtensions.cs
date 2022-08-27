// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Extensions
{
    public static class JObjectExtensions
    {
        /// <summary>
        /// Extract last update day information from resource.
        /// </summary>
        /// <param name="resource">input resource.</param>
        /// <returns>The last update timestamp of the bundle.</returns>
        public static DateTime? GetLastUpdatedDay(this JObject resource)
        {
            var lastUpdateDatetime = GetLastUpdated(resource);
            if (lastUpdateDatetime == null)
            {
                return null;
            }

            var lastUpdated = (DateTimeOffset) lastUpdateDatetime;
            return new DateTime(lastUpdated.Year, lastUpdated.Month, lastUpdated.Day);
        }

        /// <summary>
        /// Extract last update information from resource.
        /// </summary>
        /// <param name="resource">input resource.</param>
        /// <returns>The last updated timestamp of the bundle.</returns>
        public static DateTimeOffset? GetLastUpdated(this JObject resource)
        {
            var result = (resource.GetValue(FhirBundleConstants.MetaKey) as JObject)?.Value<string>(FhirBundleConstants.LastUpdatedKey);
            if (result == null)
            {
                throw new FhirDataParseExeption("Failed to find lastUpdated value in resource.");
            }

            try
            {
                return DateTimeOffset.Parse(result);
            }
            catch (Exception exception)
            {
                throw new FhirDataParseExeption("Failed to parse lastUpdated value from resource.", exception);
            }
        }
    }
}