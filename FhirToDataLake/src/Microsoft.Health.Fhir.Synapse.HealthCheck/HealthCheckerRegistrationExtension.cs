﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
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

            var schemaConfiguration = services
                    .BuildServiceProvider()
                    .GetRequiredService<IOptions<SchemaConfiguration>>()
                    .Value;

            if (schemaConfiguration.EnableCustomizedSchema)
            {
                services.AddSingleton<IHealthChecker, SchemaAzureContainerRegistryHealthChecker>();
            }

            var filterLocation = services
                    .BuildServiceProvider()
                    .GetRequiredService<IOptions<FilterLocation>>()
                    .Value;

            if (filterLocation.EnableExternalFilter)
            {
                services.AddSingleton<IHealthChecker, FilterAzureContainerRegistryHealthChecker>();
            }

            services.AddSingleton<IHealthChecker, FhirServerHealthChecker>();
            services.AddSingleton<IHealthChecker, SchedulerServiceHealthChecker>();

            return services;
        }
    }
}
