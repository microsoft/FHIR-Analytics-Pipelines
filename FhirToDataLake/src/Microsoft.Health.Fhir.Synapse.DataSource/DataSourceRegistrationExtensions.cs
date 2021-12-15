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
using Microsoft.Health.Fhir.Synapse.DataSource.Api;
using Microsoft.Health.Fhir.Synapse.DataSource.Fhir;
using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;

namespace Microsoft.Health.Fhir.Synapse.DataSource
{
    public static class DataSourceRegistrationExtensions
    {
        public static IServiceCollection AddDataSource(this IServiceCollection services)
        {
            services.AddSingleton<IFhirDataSource, FhirApiDataSource>();
            services.AddSingleton<IFhirSerializer, FhirSerializer>();
            services.AddSingleton<IFhirDataClient, FhirDataClient>();
            services.AddSingleton<IFhirSpecificationProvider, R4FhirSpecificationProvider>();

            var fhirServerConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<FhirServerConfiguration>>()
                .Value;
            services.AddHttpClient<IFhirDataClient, FhirDataClient>(client =>
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
                        services.BuildServiceProvider().GetService<ILogger<FhirDataClient>>()
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
