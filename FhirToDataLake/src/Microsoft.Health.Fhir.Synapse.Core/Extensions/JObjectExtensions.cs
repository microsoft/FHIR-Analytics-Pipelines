// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Extensions
{
    public static class JObjectExtensions
    {
        /// <summary>
        /// Extract last update day information from search response bundle.
        /// </summary>
        /// <param name="bundle">input bundle.</param>
        /// <returns>lastupdate timestamp of the bundle.</returns>
        public static DateTime? GetLastUpdatedDay(this JObject bundle)
        {
            var result = (bundle.GetValue(FhirBundleConstants.MetaKey) as JObject)?.Value<string>(FhirBundleConstants.LastUpdatedKey);
            if (result == null)
            {
                return null;
            }

            var lastUpdateDatetime = DateTimeOffset.Parse(result.ToString());
            return new DateTime(lastUpdateDatetime.Year, lastUpdateDatetime.Month, lastUpdateDatetime.Day);
        }
    }
}
