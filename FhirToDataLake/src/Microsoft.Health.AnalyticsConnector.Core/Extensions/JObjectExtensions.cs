// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.Core.Fhir;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.AnalyticsConnector.Core.Extensions
{
    public static class JObjectExtensions
    {
        /// <summary>
        /// Extract last updated timestamp information from resource.
        /// </summary>
        /// <param name="resource">input resource.</param>
        /// <returns>The last updated timestamp of the resource.</returns>
        public static DateTimeOffset GetLastUpdated(this JObject resource)
        {
            try
            {
                DateTimeOffset? lastUpdated = (resource.GetValue(FhirBundleConstants.MetaKey) as JObject)?.Value<DateTime>(FhirBundleConstants.LastUpdatedKey);

                return (DateTimeOffset)(lastUpdated == null
                    ? throw new FhirDataParseException("Failed to find lastUpdated value in resource.")
                    : lastUpdated);
            }
            catch (FhirDataParseException)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw new FhirDataParseException("Failed to parse lastUpdated value from resource.", exception);
            }
        }
    }
}