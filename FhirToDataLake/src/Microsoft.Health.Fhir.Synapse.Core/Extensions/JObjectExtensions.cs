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
        /// Extract last updated timestamp information from resource.
        /// </summary>
        /// <param name="resource">input resource.</param>
        /// <returns>The last updated timestamp of the resource.</returns>
        public static DateTimeOffset? GetLastUpdated(this JObject resource)
        {
            string result =
                (resource.GetValue(FhirBundleConstants.MetaKey) as JObject)?.Value<string>(FhirBundleConstants.LastUpdatedKey);
            if (result == null)
            {
                throw new FhirDataParseException("Failed to find lastUpdated value in resource.");
            }

            try
            {
                return DateTimeOffset.Parse(result);
            }
            catch (Exception exception)
            {
                throw new FhirDataParseException("Failed to parse lastUpdated value from resource.", exception);
            }
        }
    }
}