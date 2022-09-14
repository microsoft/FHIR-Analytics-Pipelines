// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
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

            SchemaConfiguration schemaConfiguration;
            try
            {
                schemaConfiguration = services
                    .BuildServiceProvider()
                    .GetRequiredService<IOptions<SchemaConfiguration>>()
                    .Value;
            }
            catch (Exception ex)
            {
                throw new ConfigurationErrorException("Failed to parse schema configuration", ex);
            }

            if (schemaConfiguration.EnableCustomizedSchema)
            {
                services.AddSingleton<IHealthChecker, AzureContainerRegistryHealthChecker>();
            }

            services.AddSingleton<IHealthChecker, FhirServerHealthChecker>();
            return services;
        }
    }
}
