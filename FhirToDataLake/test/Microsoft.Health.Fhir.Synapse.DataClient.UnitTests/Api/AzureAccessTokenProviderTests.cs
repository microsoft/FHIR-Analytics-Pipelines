// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Api
{
    [Trait("Category", "AccessTokenTests")]
    public class AzureAccessTokenProviderTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        [Theory]
        [InlineData("")]
        [InlineData("    ")]
        public async Task GivenAnInvalidResourceUrl_WhenGetAccessToken_ArgumentExceptionShouldBeThrown(string resourceUrl)
        {
            var accessTokenProvider = new AzureAccessTokenProvider(new MockTokenCredential(), _diagnosticLogger, new NullLogger<AzureAccessTokenProvider>());

            _ = await Assert.ThrowsAsync<ArgumentException>(() => accessTokenProvider.GetAccessTokenAsync(resourceUrl));
        }

        [Theory]
        [InlineData(null)]
        public async Task GivenANullResourceUrl_WhenGetAccessToken_ArgumentNullExceptionShouldBeThrown(string resourceUrl)
        {
            var accessTokenProvider = new AzureAccessTokenProvider(new MockTokenCredential(), _diagnosticLogger, new NullLogger<AzureAccessTokenProvider>());

            _ = await Assert.ThrowsAsync<ArgumentNullException>(() => accessTokenProvider.GetAccessTokenAsync(resourceUrl));
        }

        [Fact]
        public async Task GivenAResourceUrl_WhenGetAccessToken_CachedAccessTokenWillBeReturnedIfNotExpired()
        {
            string resourceUrl = "http://test";
            var accessTokenProvider = new AzureAccessTokenProvider(new MockTimeBasedTokenCredential(), _diagnosticLogger, new NullLogger<AzureAccessTokenProvider>());

            string accessToken = await accessTokenProvider.GetAccessTokenAsync(resourceUrl);
            Thread.Sleep(2000);
            string cachedToken = await accessTokenProvider.GetAccessTokenAsync(resourceUrl);
            Assert.Equal(accessToken, cachedToken);
        }
    }
}
