// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Core;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests
{
    public class MockCredentialProvider : IFhirCredentialProvider
    {
        public TokenCredential GetCredential()
        {
            return new MockCredential();
        }
    }

    internal class MockCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(new AccessToken(TestDataConstants.TestAccessToken, DateTimeOffset.Now.AddMinutes(10)));
        }
    }
}
