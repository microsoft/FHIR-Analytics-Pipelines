// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;

namespace Microsoft.Health.Fhir.Synapse.DataClient
{
    public static class DataClientRegistrationExtensions
    {
        public static IServiceCollection AddDataSource(this IServiceCollection services)
        {
            services.AddSingleton<IFhirApiDataSource, FhirApiDataSource>();
            services.AddSingleton<IFhirDataClient, FhirApiDataClient>();
            services.AddSingleton<IAccessTokenProvider, AzureAccessTokenProvider>();

            var fhirServerConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<FhirServerConfiguration>>()
                .Value;
            services.AddHttpClient<IFhirDataClient, FhirApiDataClient>(client =>
            {
                client.BaseAddress = new Uri(fhirServerConfiguration.ServerUrl);
            })
            .AddPolicyHandler(GetRetryPolicy(services))
            .AddPolicyHandler(GetTimeoutPolicy())
            .AddPolicyHandler(GetCircuitBreakerPolicy());

            return services;
        }

        private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(IServiceCollection services)
        {
            return HttpPolicyExtensions
                .HandleTransientHttpError()
                .Or<TimeoutRejectedException>()
                .OrResult(msg => msg.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                .WaitAndRetryAsync(
                    5,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    onRetry: (outcome, timespan, retryAttempt, context) =>
                    {
                        services.BuildServiceProvider().GetService<ILogger<FhirApiDataClient>>()
                            .LogWarning("Searching FHIR data failed, delaying for {delay}ms, then making retry {retry}.", timespan.TotalMilliseconds, retryAttempt);
                    });
        }

        private static IAsyncPolicy<HttpResponseMessage> GetCircuitBreakerPolicy()
        {
            return HttpPolicyExtensions
                .HandleTransientHttpError()
                .CircuitBreakerAsync(5, TimeSpan.FromSeconds(30));
        }

        private static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy(double seconds = 60)
        {
            return Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(seconds));
        }
    }
}
