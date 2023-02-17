// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.Core.Fhir;
using Newtonsoft.Json;
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
        public static DateTimeOffset? GetLastUpdated(this JObject resource)
        {
            try
            {
                var lastUpdatedObject = JObject.FromObject(
                    resource.GetValue(FhirBundleConstants.MetaKey),
                    new JsonSerializer
                    {
                        DateParseHandling = DateParseHandling.None,
                    });
                var result = JsonConvert.DeserializeObject<Dictionary<string, string>>(lastUpdatedObject.ToString()).GetValueOrDefault(FhirBundleConstants.LastUpdatedKey);
                if (result == null)
                {
                    throw new FhirDataParseException("Failed to find lastUpdated value in resource.");
                }

                return DateTimeOffset.Parse(result);
            }
            catch (Exception exception)
            {
                if (exception is FhirDataParseException)
                {
                    throw;
                }

                throw new FhirDataParseException("Failed to parse lastUpdated value from resource.", exception);
            }
        }
    }
}