// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.UnitTests.ContainerRegistry
{
    public class ContainerRegistryAccessTokenProviderTests
    {
        private const string RegistryServer = "test.azurecr.io";

        public ContainerRegistryAccessTokenProviderTests()
        {
        }

        [Fact]
        public async Task GivenARegistry_WithoutRbacGranted_WhenGetToken_ExceptionShouldBeThrown()
        {
            var acrTokenProvider = GetMockAcrTokenProvider(HttpStatusCode.Unauthorized);

            await Assert.ThrowsAsync<ContainerRegistryTokenException>(() => acrTokenProvider.GetTokenAsync(RegistryServer, default));
        }

        [Fact]
        public async Task GivenANotFoundRegistry_WhenGetToken_ExceptionShouldBeThrown()
        {
            var acrTokenProvider = GetMockAcrTokenProvider(HttpStatusCode.NotFound);
            await Assert.ThrowsAsync<ContainerRegistryTokenException>(() => acrTokenProvider.GetTokenAsync(RegistryServer, default));
        }

        [Theory]
        [InlineData("{\"refresh_token\":\"refresh_token_test\"}")]
        [InlineData("{\"access_token\":\"access_token_test\"}")]
        [InlineData("{\"refresh_token\":\"\", \"access_token\":\"access_token_test\"}")]
        [InlineData("{\"refresh_token\":\"refresh_token_test\", \"access_token\":\"\"}")]
        public async Task GivenAValidRegistry_WhenRefreshTokenIsEmpty_ExceptionShouldBeThrown(string content)
        {
            var acrTokenProvider = GetMockAcrTokenProvider(HttpStatusCode.OK, content);
            await Assert.ThrowsAsync<ContainerRegistryTokenException>(() => acrTokenProvider.GetTokenAsync(RegistryServer, default));
        }

        [Fact]
        public async Task GivenAValidRegistry_WhenGetToken_CorrectResultShouldBeReturned()
        {
            var acrTokenProvider = GetMockAcrTokenProvider(HttpStatusCode.OK, "{\"refresh_token\":\"refresh_token_test\", \"access_token\":\"access_token_test\"}");
            string accessToken = await acrTokenProvider.GetTokenAsync(RegistryServer, default);
            Assert.Equal("Bearer access_token_test", accessToken);
        }

        private ContainerRegistryAccessTokenProvider GetMockAcrTokenProvider(HttpStatusCode statusCode, string content = "")
        {
            ITokenCredentialProvider tokenProvider = Substitute.For<ITokenCredentialProvider>();

            // tokenProvider.GetAccessTokenAsync(default, default).ReturnsForAnyArgs("Bearer test");
            var httpClient = new HttpClient(new MockHttpMessageHandler(content, statusCode));
            return new ContainerRegistryAccessTokenProvider(tokenProvider, httpClient, new NullLogger<ContainerRegistryAccessTokenProvider>());
        }

        internal class MockHttpMessageHandler : HttpMessageHandler
        {
            private readonly string _response;
            private readonly HttpStatusCode _statusCode;

            public MockHttpMessageHandler(string response, HttpStatusCode statusCode)
            {
                _response = response;
                _statusCode = statusCode;
            }

            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                return Task.FromResult(new HttpResponseMessage
                {
                    StatusCode = _statusCode,
                    Content = new StringContent(_response),
                });
            }
        }
    }
}
