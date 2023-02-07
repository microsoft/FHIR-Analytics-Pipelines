// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.AnalyticsConnector.DataClient.Models;

namespace Microsoft.Health.AnalyticsConnector.DataClient
{
    public interface IApiDataClient
    {
        /// <summary>
        /// Returns a response content which contains the matching search results.
        /// </summary>
        /// <param name="serverApiOptions">server api options.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>returned bundle.</returns>
        public Task<string> SearchAsync(
            BaseApiOptions serverApiOptions,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Returns a response content which contains the matching search results.
        /// </summary>
        /// <param name="serverApiOptions">server api options.</param>
        /// <returns>returned bundle.</returns>
        public string Search(BaseApiOptions serverApiOptions);
    }
}