// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Core;
using Microsoft.Health.AnalyticsConnector.Common.Authentication;

namespace Microsoft.Health.AnalyticsConnector.DataClient.UnitTests.Api
{
    public class MockTokenCredentialProvider : ITokenCredentialProvider
    {
        public TokenCredential GetCredential(TokenCredentialTypes type)
        {
            return new MockTokenCredential();
        }
    }
}
