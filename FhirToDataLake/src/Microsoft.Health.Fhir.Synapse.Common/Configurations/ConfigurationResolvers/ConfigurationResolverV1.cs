// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations.ConfigurationResolvers
{
    public class ConfigurationResolverV1
    {
        public static void Resolve(
            IServiceCollection services,
            IConfiguration configuration)
        {
            if (configuration.GetSection(ConfigurationConstants.DataSourceConfigurationKey).Exists())
            {
                // Try to get DataSourceConfiguration from configuration.
                services.Configure<DataSourceConfiguration>(options =>
                    configuration.GetSection(ConfigurationConstants.DataSourceConfigurationKey).Bind(options));
            }
            else
            {
                // If DataSourceConfiguration is not found, find FhirServerConfiguration instead
                // and create a DataSourceConfiguration from it for backward compatibility.
                services.Configure<DataSourceConfiguration>(options =>
                    configuration.GetSection(ConfigurationConstants.FhirServerConfigurationKey).Bind(options.FhirServer));
            }

            services.Configure<JobConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.JobConfigurationKey).Bind(options));
            services.Configure<FilterConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.FilterConfigurationKey).Bind(options));
            services.Configure<FilterLocation>(options =>
                configuration.GetSection(ConfigurationConstants.FilterConfigurationKey).Bind(options));
            services.Configure<DataLakeStoreConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.DataLakeStoreConfigurationKey).Bind(options));
            services.Configure<ArrowConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.ArrowConfigurationKey).Bind(options));
            services.Configure<SchemaConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.SchemaConfigurationKey).Bind(options));
            services.Configure<HealthCheckConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.HealthCheckConfigurationKey).Bind(options));
            services.Configure<StorageConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.StorageConfigurationKey).Bind(options));
        }
    }
}
