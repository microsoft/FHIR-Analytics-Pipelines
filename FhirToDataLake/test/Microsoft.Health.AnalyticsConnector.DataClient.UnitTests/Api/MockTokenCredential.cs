// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;

namespace Microsoft.Health.AnalyticsConnector.DataClient.UnitTests.Api
{
    public class MockTokenCredential : TokenCredential
    {
        public List<string> Scopes { get; set; } = null;

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            // Is Audience is not null or empty, will check request scopes and compare them with Audience
            if (Scopes != null && !Scopes.Intersect(requestContext.Scopes).Any())
            {
                throw new AuthenticationFailedException($"The resource principal named {string.Join(",", requestContext.Scopes)} was not found.");
            }

            return ValueTask.FromResult(new AccessToken(TestDataConstants.TestAccessToken, DateTimeOffset.Now.AddMinutes(5.1)));
        }
    }
}
