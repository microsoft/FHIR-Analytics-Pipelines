// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Api
{
    [Trait("Category", "AccessTokenTests")]
    public class AzureAccessTokenProviderTests
    {
        [Theory]
        [InlineData("")]
        [InlineData("    ")]
        public async Task GivenAnInvalidResourceUrl_WhenGetAccessToken_ArgumentExceptionShouldBeThrown(string resourceUrl)
        {
            var accessTokenProvider = new AzureAccessTokenProvider(new InternalFhirCredentialProvider(new Logger<InternalFhirCredentialProvider>(new LoggerFactory())), new Logger<AzureAccessTokenProvider>(new LoggerFactory()));

            _ = await Assert.ThrowsAsync<ArgumentException>(() => accessTokenProvider.GetAccessTokenAsync(resourceUrl));
        }

        [Theory]
        [InlineData(null)]
        public async Task GivenANullResourceUrl_WhenGetAccessToken_ArgumentNullExceptionShouldBeThrown(string resourceUrl)
        {
            var accessTokenProvider = new AzureAccessTokenProvider(new InternalFhirCredentialProvider(new Logger<InternalFhirCredentialProvider>(new LoggerFactory())), new Logger<AzureAccessTokenProvider>(new LoggerFactory()));

            _ = await Assert.ThrowsAsync<ArgumentNullException>(() => accessTokenProvider.GetAccessTokenAsync(resourceUrl));
        }
    }
}
