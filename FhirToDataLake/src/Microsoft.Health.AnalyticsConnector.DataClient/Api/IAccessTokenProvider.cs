// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Api
{
    public interface IAccessTokenProvider
    {
        public Task<string> GetAccessTokenAsync(
            string resourceUrl,
            CancellationToken cancellationToken = default);
    }
}
