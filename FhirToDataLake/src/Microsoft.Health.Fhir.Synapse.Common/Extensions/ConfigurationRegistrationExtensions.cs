// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
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
            FhirServerConfiguration fhirServerConfiguration;
            try
            {
                // include enum field, an exception will be thrown when parse invalid enum string
                fhirServerConfiguration = services
                    .BuildServiceProvider()
                    .GetRequiredService<IOptions<FhirServerConfiguration>>()
                    .Value;
            }
            catch (Exception ex)
            {
                throw new ConfigurationErrorException("Failed to parse fhir server configuration", ex);
            }

            if (string.IsNullOrEmpty(fhirServerConfiguration.ServerUrl))
            {
                throw new ConfigurationErrorException($"Fhir server url can not be empty.");
            }

            if (fhirServerConfiguration.Version != FhirVersion.R4)
            {
                throw new ConfigurationErrorException($"Fhir version {fhirServerConfiguration.Version} is not supported.");
            }

            JobConfiguration jobConfiguration;
            try
            {
                jobConfiguration = services
                    .BuildServiceProvider()
                    .GetRequiredService<IOptions<JobConfiguration>>()
                    .Value;
            }
            catch (Exception ex)
            {
                throw new ConfigurationErrorException("Failed to parse job configuration", ex);
            }

            if (string.IsNullOrEmpty(jobConfiguration.ContainerName))
            {
                throw new ConfigurationErrorException($"Target azure container name can not be empty.");
            }

            FilterConfiguration filterConfiguration;
            try
            {
                filterConfiguration = services
                    .BuildServiceProvider()
                    .GetRequiredService<IOptions<FilterConfiguration>>()
                    .Value;
            }
            catch (Exception ex)
            {
                throw new ConfigurationErrorException("Failed to parse filter configuration", ex);
            }

            ValidateFilterConfiguration(filterConfiguration);

            var storeConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<DataLakeStoreConfiguration>>()
                .Value;

            if (string.IsNullOrEmpty(storeConfiguration.StorageUrl))
            {
                throw new ConfigurationErrorException($"Target azure storage url can not be empty.");
            }

            var schemaConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<SchemaConfiguration>>()
                .Value;

            if (string.IsNullOrWhiteSpace(schemaConfiguration.SchemaImageReference))
            {
                if (schemaConfiguration.EnableCustomizedSchema)
                {
                    throw new ConfigurationErrorException($"Customized schema image reference can not be empty when customized schema is enable.");
                }
            }
            else
            {
                ValidateImageReference(schemaConfiguration.SchemaImageReference);
            }
        }

        private static void ValidateImageReference(string imageReference)
        {
            var registryDelimiterPosition = imageReference.IndexOf(ConfigurationConstants.ImageRegistryDelimiter, StringComparison.InvariantCultureIgnoreCase);
            if (registryDelimiterPosition <= 0 || registryDelimiterPosition == imageReference.Length - 1)
            {
                throw new ConfigurationErrorException("Customized schema image format is invalid: registry server is missing.");
            }

            imageReference = imageReference[(registryDelimiterPosition + 1) ..];
            string imageName = imageReference;
            if (imageReference.Contains(ConfigurationConstants.ImageDigestDelimiter, StringComparison.OrdinalIgnoreCase))
            {
                Tuple<string, string> imageMeta = ParseImageMeta(imageReference, ConfigurationConstants.ImageDigestDelimiter);
                if (string.IsNullOrEmpty(imageMeta.Item1) || string.IsNullOrEmpty(imageMeta.Item2))
                {
                    throw new ConfigurationErrorException("Customized schema image format is invalid: digest is missing.");
                }

                imageName = imageMeta.Item1;
            }
            else if (imageReference.Contains(ConfigurationConstants.ImageTagDelimiter, StringComparison.OrdinalIgnoreCase))
            {
                Tuple<string, string> imageMeta = ParseImageMeta(imageReference, ConfigurationConstants.ImageTagDelimiter);
                if (string.IsNullOrEmpty(imageMeta.Item1) || string.IsNullOrEmpty(imageMeta.Item2))
                {
                    throw new ConfigurationErrorException("Customized schema image reference format is invalid: tag is missing.");
                }

                imageName = imageMeta.Item1;
            }

            ValidateImageName(imageName);
        }

        private static Tuple<string, string> ParseImageMeta(string input, char delimiter)
        {
            var index = input.IndexOf(delimiter, StringComparison.InvariantCultureIgnoreCase);
            return new Tuple<string, string>(input[0..index], input[(index + 1) ..]);
        }

        private static void ValidateImageName(string imageName)
        {
            if (!ConfigurationConstants.ImageNameRegex.IsMatch(imageName))
            {
                throw new ConfigurationErrorException(@"Customized schema image name is invalid. Image name should contains lowercase letters, digits and separators. The valid format is ^[a-z0-9]+(([_\.]|_{2}|\-+)[a-z0-9]+)*(\/[a-z0-9]+(([_\.]|_{2}|\-+)[a-z0-9]+)*)*$");
            }
        }

        private static void ValidateFilterConfiguration(FilterConfiguration filterConfiguration)
        {
            if (!Enum.IsDefined(typeof(FilterScope), filterConfiguration.FilterScope))
            {
                throw new ConfigurationErrorException(
                    $"Filter Scope '{filterConfiguration.FilterScope}' is not supported.");
            }

            if (filterConfiguration.FilterScope == FilterScope.Group && string.IsNullOrWhiteSpace(filterConfiguration.GroupId))
            {
                throw new ConfigurationErrorException("Group id can not be null, empty or white space for `Group` filter scope.");
            }
        }
    }
}