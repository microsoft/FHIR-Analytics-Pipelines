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
        /// <returns>lastupdate timestamp of the bundle.</returns>
        public static DateTime? GetLastUpdatedDay(this JObject resource)
        {
            var result = (resource.GetValue(FhirBundleConstants.MetaKey) as JObject)?.Value<string>(FhirBundleConstants.LastUpdatedKey);
            if (result == null)
            {
                throw new FhirDataParseExeption("Failed to find lastUpdated value in resource.");
            }

            try
            {
                var lastUpdateDatetime = DateTimeOffset.Parse(result.ToString());
                return new DateTime(lastUpdateDatetime.Year, lastUpdateDatetime.Month, lastUpdateDatetime.Day);
            }
            catch (Exception exception)
            {
                throw new FhirDataParseExeption("Failed to parse lastUpdated value from resource.", exception);
            }
        }
    }
}
