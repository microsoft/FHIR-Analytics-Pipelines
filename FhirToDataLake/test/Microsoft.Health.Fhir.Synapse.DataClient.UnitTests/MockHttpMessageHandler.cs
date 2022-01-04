// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests
{
    public class MockHttpMessageHandler : HttpMessageHandler
    {
        private IDictionary<string, HttpResponseMessage> _sampleRequests;

        public MockHttpMessageHandler(IDictionary<string, HttpResponseMessage> sampleRequests)
            : base()
        {
            _sampleRequests = sampleRequests;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var accessToken = request.Headers.Authorization.Parameter;
            if (!string.Equals(accessToken, TestDataConstants.TestAccessToken))
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Unauthorized));
            }

            var urlKey = request.RequestUri.ToString();
            if (_sampleRequests.ContainsKey(urlKey))
            {
                return Task.FromResult(_sampleRequests[urlKey]);
            }

            throw new HttpRequestException();
        }
    }
}
