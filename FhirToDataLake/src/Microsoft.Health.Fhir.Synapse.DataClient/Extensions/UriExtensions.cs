// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Web;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Extensions
{
    public static class UriExtensions
    {
        /// <summary>
        /// Add query string to uri.
        /// </summary>
        /// <param name="uri">origin uri.</param>
        /// <param name="parameters">parameters to append.</param>
        /// <returns>new Uri with expected query string.</returns>
        public static Uri AddQueryString(this Uri uri, IEnumerable<KeyValuePair<string, string>> parameters)
        {
            var uriBuilder = new UriBuilder(uri);
            var query = HttpUtility.ParseQueryString(uriBuilder.Query);
            foreach (var parameter in parameters)
            {
                query.Add(parameter.Key, parameter.Value);
            }

            uriBuilder.Query = query.ToString();

            return uriBuilder.Uri;
        }
    }
}
