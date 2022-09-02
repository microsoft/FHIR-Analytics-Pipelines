// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Linq;
using EnsureThat;
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

            if (string.IsNullOrWhiteSpace(fhirServerConfiguration.ServerUrl))
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

            ValidateAgentName(jobConfiguration.AgentName);

            EnsureArg.EnumIsDefined(jobConfiguration.QueueType, nameof(jobConfiguration.QueueType));

            if (string.IsNullOrWhiteSpace(jobConfiguration.TableUrl))
            {
                throw new ConfigurationErrorException($"Table Url can not be empty.");
            }

            if (string.IsNullOrWhiteSpace(jobConfiguration.QueueUrl))
            {
                throw new ConfigurationErrorException($"Queue Url can not be empty.");
            }

            if (string.IsNullOrWhiteSpace(jobConfiguration.SchedulerCronExpression))
            {
                throw new ConfigurationErrorException($"Scheduler crontab expression can not be empty.");
            }

            if (string.IsNullOrWhiteSpace(jobConfiguration.ContainerName))
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

            if (string.IsNullOrWhiteSpace(storeConfiguration.StorageUrl))
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

        // agent name is used as part of table name and queue name, need to validate agent name to contain only alphanumeric characters, and not begin with a numeric character.
        // Reference: https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#table-names
        // https://docs.microsoft.com/en-us/rest/api/storageservices/naming-queues-and-metadata#queue-names
        public static void ValidateAgentName(string agentName)
        {

            if (string.IsNullOrWhiteSpace(agentName))
            {
                throw new ConfigurationErrorException("Agent name can not be empty.");
            }

            if (!agentName.All(char.IsLetterOrDigit))
            {
                throw new ConfigurationErrorException("Agent name may contain only alphanumeric characters.");
            }

            if (!char.IsLetter(agentName.First()))
            {
                throw new ConfigurationErrorException("Agent name should begin with an alphabet character.");
            }

            // the table/queue name must be from 3 to 63 characters long, we add suffix to the agent name as table/queue name: {agentName}metadatatable, {agentName}jobinfotable, {agentName}jobinfoqueue
            if (agentName.Length >= 50)
            {
                throw new ConfigurationErrorException("Agent name should less than 50 characters long.");
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
