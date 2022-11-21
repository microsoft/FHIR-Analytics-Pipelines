// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Net;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Api;
using Microsoft.Health.Fhir.Synapse.DataClient.Api.Dicom;
using Polly;
using Polly.Extensions.Http;
using Polly.Timeout;

namespace Microsoft.Health.Fhir.Synapse.DataClient
{
    public static class DataClientRegistrationExtensions
    {
        public static IServiceCollection AddDataSource(this IServiceCollection services)
        {
            services.AddSingleton<IApiDataSource, ApiDataSource>();
            services.AddSingleton<IFhirDataClient, FhirApiDataClient>();

            var dataSourceConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<DataSourceConfiguration>>()
                .Value;

            switch (dataSourceConfiguration.Type)
            {
                case DataSourceType.FHIR:
                    services.AddHttpClient<IFhirDataClient, FhirApiDataClient>(client =>
                    {
                        client.BaseAddress = new Uri(dataSourceConfiguration.FhirServer.ServerUrl);
                    })
                    .AddPolicyHandler(GetRetryPolicy(services))
                    .AddPolicyHandler(GetTimeoutPolicy())
                    .AddPolicyHandler(GetCircuitBreakerPolicy());

                    break;
                case DataSourceType.DICOM:
                    services.AddHttpClient<IDicomDataClient, DicomApiDataClient>(client =>
                    {
                        client.BaseAddress = new Uri(dataSourceConfiguration.DicomServer.ServerUrl);
                    })
                    .AddPolicyHandler(GetRetryPolicy(services))
                    .AddPolicyHandler(GetTimeoutPolicy())
                    .AddPolicyHandler(GetCircuitBreakerPolicy());

                    break;
                default:
                    throw new ConfigurationErrorException($"Data source type {dataSourceConfiguration.Type} is not supported");
            }

            return services;
        }

        private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy(IServiceCollection services)
        {
            return HttpPolicyExtensions
                .HandleTransientHttpError()
                .Or<TimeoutRejectedException>()
                .OrResult(msg => msg.StatusCode == HttpStatusCode.TooManyRequests)
                .WaitAndRetryAsync(
                    5,
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    onRetry: (outcome, timespan, retryAttempt, context) =>
                    {
                        services.BuildServiceProvider().GetService<ILogger<FhirApiDataClient>>()
                            .LogInformation("Searching FHIR data failed, delaying for {delay}ms, then making retry {retry}.", timespan.TotalMilliseconds, retryAttempt);
                    });
        }

        private static IAsyncPolicy<HttpResponseMessage> GetCircuitBreakerPolicy()
        {
            return HttpPolicyExtensions
                .HandleTransientHttpError()
                .CircuitBreakerAsync(40, TimeSpan.FromSeconds(30));
        }

        private static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy(double seconds = 60)
        {
            return Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(seconds));
        }
    }
}
