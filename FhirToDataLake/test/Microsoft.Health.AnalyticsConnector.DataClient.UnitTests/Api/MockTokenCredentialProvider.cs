// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Azure.Core;
using Microsoft.Health.AnalyticsConnector.Common.Authentication;

namespace Microsoft.Health.AnalyticsConnector.DataClient.UnitTests.Api
{
    public class MockTokenCredentialProvider : ITokenCredentialProvider
    {
        public string Audience { get; set; } = string.Empty;

        public TokenCredential GetCredential(TokenCredentialTypes type)
        {
            var mockTokenCredential = new MockTokenCredential();
            if (!string.IsNullOrEmpty(Audience))
            {
                mockTokenCredential.Scopes = new List<string>() { Audience.TrimEnd('/') + "/.default" };
            }

            return mockTokenCredential;
        }
    }
}
