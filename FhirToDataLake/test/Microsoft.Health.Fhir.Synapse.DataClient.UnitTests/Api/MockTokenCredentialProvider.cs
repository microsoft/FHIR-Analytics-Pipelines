// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Core;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Api
{
    public class MockTokenCredentialProvider : ITokenCredentialProvider
    {
        public TokenCredential GetCredential(TokenCredentialTypes type)
        {
            return new MockTokenCredential();
        }
    }
}
