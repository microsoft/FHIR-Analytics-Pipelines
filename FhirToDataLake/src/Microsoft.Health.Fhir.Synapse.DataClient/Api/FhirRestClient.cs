using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Identity;
using EnsureThat;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public class FhirRestClient
    {

        private readonly ILogger<FhirRestClient> _logger;
        private readonly HttpPipelinePolicy _pipelinePolicy;
        private readonly HttpPipeline _pipeline;
        
        public FhirRestClient(
            Uri storageUri, TokenCredential credential)
        {
            EnsureArg.IsNotNull(storageUri, nameof(storageUri));

            _pipelinePolicy = new BearerTokenAuthenticationPolicy(credential, "");
            _pipeline = HttpPipelineBuilder.Build(ClientOptions.Default, _pipelinePolicy);
        }

        public  async ValueTask<string> QueryAsync(
                Uri resourceUri,
                bool async = true,
                string operationName = "BlobClient.Query",
                CancellationToken cancellationToken = default)
        {
            try
            {
                using (HttpMessage _message = QueryAsync_CreateMessage(
                    resourceUri
                    ))
                {
                    // Avoid buffering if stream is going to be returned to the caller
                    _message.BufferResponse = false;
                    if (async)
                    {
                        // Send the request asynchronously if we're being called via an async path
                        await _pipeline.SendAsync(_message, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        // Send the request synchronously through the API that blocks if we're being called via a sync path
                        // (this is safe because the Task will complete before the user can call Wait)
                        _pipeline.Send(_message, cancellationToken);
                    }
                    Response _response = _message.Response;
                    cancellationToken.ThrowIfCancellationRequested();
                    return _message.ExtractResponseContent().ToString();
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        internal HttpMessage QueryAsync_CreateMessage(
                Uri resourceUri
                )
        {
            // Validation
            if (resourceUri == null)
            {
                throw new System.ArgumentNullException(nameof(resourceUri));
            }

            // Create the request
            HttpMessage _message = _pipeline.CreateMessage();
            Request _request = _message.Request;

            // Set the endpoint
            _request.Method = RequestMethod.Post;
            _request.Uri.Reset(resourceUri);

            // Add request headers

            // Create the body

            return _message;
        }
    }
}
