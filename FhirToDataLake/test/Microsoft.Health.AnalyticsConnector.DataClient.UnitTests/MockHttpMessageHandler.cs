// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Health.AnalyticsConnector.DataClient.UnitTests
{
    public class MockHttpMessageHandler : HttpMessageHandler
    {
        private IDictionary<string, HttpResponseMessage> _sampleRequests;
        private HttpStatusCode _statusCode;

        public MockHttpMessageHandler(IDictionary<string, HttpResponseMessage> sampleRequests, HttpStatusCode statusCode = HttpStatusCode.OK)
            : base()
        {
            _sampleRequests = sampleRequests;
            _statusCode = statusCode;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.RequestUri == null)
            {
                throw new HttpRequestException();
            }

            if (_statusCode != HttpStatusCode.OK)
            {
                throw new HttpRequestException(string.Empty, new Exception(), _statusCode);
            }

            if (request.RequestUri.AbsolutePath != "/metadata")
            {
                string accessToken = request.Headers.Authorization.Parameter;
                if (!string.Equals(accessToken, TestDataConstants.TestAccessToken))
                {
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Unauthorized));
                }
            }

            string urlKey = request.RequestUri.ToString();
            if (_sampleRequests.ContainsKey(urlKey))
            {
                return Task.FromResult(_sampleRequests[urlKey]);
            }

            throw new HttpRequestException();
        }

        protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (_statusCode != HttpStatusCode.OK)
            {
                throw new HttpRequestException(string.Empty, new Exception(), _statusCode);
            }

            if (request.RequestUri != null)
            {
                string urlKey = request.RequestUri.ToString();
                if (_sampleRequests.ContainsKey(urlKey))
                {
                    return _sampleRequests[urlKey];
                }
            }

            throw new HttpRequestException();
        }
    }
}
