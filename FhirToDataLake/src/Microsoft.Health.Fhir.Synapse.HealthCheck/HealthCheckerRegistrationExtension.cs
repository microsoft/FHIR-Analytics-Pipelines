// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck
{
    public static class HealthCheckerRegistrationExtension
    {
        public static IServiceCollection AddHealthCheckService(
            this IServiceCollection services)
        {
            services.AddHostedService<HealthCheckBackgroundService>();

            services.AddSingleton<IHealthCheckEngine, HealthCheckEngine>();

            services.AddSingleton<IHealthChecker, AzureBlobStorageHealthChecker>();

            services.AddSingleton<IHealthChecker, FhirServerHealthChecker>();
            return services;
        }
    }
}
