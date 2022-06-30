// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Common.Extensions
{
    public static class ConfigurationRegistrationExtensions
    {
        public static IServiceCollection AddConfiguration(
            this IServiceCollection services,
            IConfiguration configuration)
        {
            services.Configure<FhirServerConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.FhirServerConfigurationKey).Bind(options));
            services.Configure<JobConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.JobConfigurationKey).Bind(options));
            services.Configure<FilterConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.FilterConfigurationKey).Bind(options));
            services.Configure<DataLakeStoreConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.DataLakeStoreConfigurationKey).Bind(options));
            services.Configure<ArrowConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.ArrowConfigurationKey).Bind(options));
            services.Configure<JobSchedulerConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.SchedulerConfigurationKey).Bind(options));
            services.Configure<SchemaConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.SchemaConfigurationKey).Bind(options));

            // Validates the input configs.
            services.ValidateConfiguration();
            return services;
        }

        private static void ValidateConfiguration(this IServiceCollection services)
        {
            var fhirServerConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<FhirServerConfiguration>>()
                .Value;

            if (string.IsNullOrEmpty(fhirServerConfiguration.ServerUrl))
            {
                throw new ConfigurationErrorException($"Fhir server url '{fhirServerConfiguration.ServerUrl}' can not be empty.");
            }

            if (fhirServerConfiguration.Version != FhirVersion.R4)
            {
                throw new ConfigurationErrorException($"Fhir version {fhirServerConfiguration.Version} is not supported.");
            }

            var jobConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<JobConfiguration>>()
                .Value;

            if (string.IsNullOrEmpty(jobConfiguration.ContainerName))
            {
                throw new ConfigurationErrorException($"Target azure container name '{jobConfiguration.ContainerName}' can not be empty.");
            }

            var filterConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<FilterConfiguration>>()
                .Value;

            ValidateTypeFilterConfiguration(filterConfiguration);

            var storeConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<DataLakeStoreConfiguration>>()
                .Value;

            if (string.IsNullOrEmpty(storeConfiguration.StorageUrl))
            {
                throw new ConfigurationErrorException($"Target azure storage url '{storeConfiguration.StorageUrl}' can not be empty.");
            }
        }

        private static void ValidateTypeFilterConfiguration(FilterConfiguration filterConfiguration)
        {
            if (filterConfiguration.FilterScope != FilterScope.System && filterConfiguration.FilterScope != FilterScope.Group)
            {
                throw new ConfigurationErrorException(
                    $"Filter Scope '{filterConfiguration.FilterScope}' is not supported.");
            }

            if (filterConfiguration.FilterScope == FilterScope.Group && string.IsNullOrEmpty(filterConfiguration.GroupId))
            {
                throw new ConfigurationErrorException($"Group id '{filterConfiguration.GroupId}' can not be empty for `Group` scope.");
            }
        }
    }
}