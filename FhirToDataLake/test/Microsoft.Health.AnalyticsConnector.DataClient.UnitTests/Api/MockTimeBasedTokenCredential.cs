﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;

namespace Microsoft.Health.AnalyticsConnector.DataClient.UnitTests.Api
{
    public class MockTimeBasedTokenCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            string time = DateTimeOffset.Now.ToString();
            return ValueTask.FromResult(new AccessToken(TestDataConstants.TestAccessToken + time, DateTimeOffset.Now.AddMinutes(5.1)));
        }
    }
}
