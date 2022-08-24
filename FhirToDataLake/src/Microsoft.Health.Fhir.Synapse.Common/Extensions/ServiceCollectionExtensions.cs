// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Common.Extensions
{
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Validate configuration in service collection.
        /// </summary>
        /// <param name="services">Service collection instance.</param>
        /// <exception cref="ConfigurationErrorException">Throw ConfigurationErrorException if configuration is invalid.</exception>
        public static void ValidateConfiguration(this IServiceCollection services)
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

            if (!ConfigurationConstants.SupportedFhirVersions.Contains(fhirServerConfiguration.Version))
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

            // TODO: add more validation for agent name, table url, queue url
            // TODO: enable it when generic task is enabled
            /*
            if (string.IsNullOrEmpty(jobConfiguration.AgentName))
            {
                throw new ConfigurationErrorException($"Agent name can not be empty.");
            }
            */

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
                if (string.IsNullOrWhiteSpace(schemaConfiguration.SchemaImageReference))
                {
                    throw new ConfigurationErrorException($"Customized schema image reference can not be empty when customized schema is enable.");
                }
                else
                {
                    ValidateImageReference(schemaConfiguration.SchemaImageReference);
                }
            }
            else
            {
                if (!string.IsNullOrWhiteSpace(schemaConfiguration.SchemaImageReference))
                {
                    throw new ConfigurationErrorException($"Found the schema image reference but customized schema is disable.");
                }
            }

            HealthCheckConfiguration healthCheckConfiguration;
            try
            {
                healthCheckConfiguration = services
                    .BuildServiceProvider()
                    .GetRequiredService<IOptions<HealthCheckConfiguration>>()
                    .Value;
            }
            catch (Exception ex)
            {
                throw new ConfigurationErrorException("Failed to parse health check configuration", ex);
            }

            if (healthCheckConfiguration.HealthCheckTimeIntervalInSeconds <= 0 ||
                healthCheckConfiguration.HealthCheckTimeoutInSeconds <= 0 ||
                healthCheckConfiguration.HealthCheckTimeIntervalInSeconds < healthCheckConfiguration.HealthCheckTimeoutInSeconds)
            {
                throw new ConfigurationErrorException("Invalid health check configuration. Health check time interval should be greater than health check timeout and both of them should greater than zero.");
            }
        }

        /// <summary>
        /// Validate the image reference.
        /// Image reference pattern: <registry>/<name>@<digest> or <registry>/<name>:<tag>
        /// E.g. testacr.azurecr.io/templatetest@sha256:412ea84f1bb1a9d98345efb7b427ba89616ec29ac332d543eff9a2161ca12a58
        /// </summary>
        /// <param name="imageReference">Image reference</param>
        /// <exception cref="ConfigurationErrorException">Throw ConfigurationErrorException if imageReference is invalid</exception>
        public static void ValidateImageReference(string imageReference)
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
                throw new ConfigurationErrorException(
                    $"Customized schema image name is invalid. Image name should contains lowercase letters, digits and separators. The valid format is {ConfigurationConstants.ImageNameRegex}");
            }
        }

        /// <summary>
        /// Validate FilterConfiguration.
        /// </summary>
        /// <param name="filterConfiguration">FilterConfiguration instance.</param>
        /// <exception cref="ConfigurationErrorException">Throw ConfigurationErrorException if filterConfiguration is invalid.</exception>
        public static void ValidateFilterConfiguration(FilterConfiguration filterConfiguration)
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
