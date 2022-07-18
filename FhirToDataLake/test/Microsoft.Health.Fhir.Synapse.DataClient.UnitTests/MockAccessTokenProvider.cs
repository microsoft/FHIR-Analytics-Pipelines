// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests
{
    public class MockAccessTokenProvider : IAccessTokenProvider
    {
        public Task<string> GetAccessTokenAsync(string resourceUrl, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(TestDataConstants.TestAccessToken);
        }
    }
}
