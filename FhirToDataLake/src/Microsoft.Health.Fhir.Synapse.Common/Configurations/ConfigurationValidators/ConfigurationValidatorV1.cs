// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.Common.Configurations.ConfigurationValidators
{
    public static class ConfigurationValidatorV1
    {
        /// <summary>
        /// Validate configuration in service collection.
        /// </summary>
        /// <param name="services">Service collection instance.</param>
        /// <exception cref="ConfigurationErrorException">Throw ConfigurationErrorException if configuration is invalid.</exception>
        public static void Validate(IServiceCollection services)
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

            ValidateUtility.ValidateQueueOrTableName(jobConfiguration.JobInfoQueueName);
            ValidateUtility.ValidateQueueOrTableName(jobConfiguration.JobInfoTableName);
            ValidateUtility.ValidateQueueOrTableName(jobConfiguration.MetadataTableName);

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

            FilterLocation filterLocation;
            try
            {
                filterLocation = services
                    .BuildServiceProvider()
                    .GetRequiredService<IOptions<FilterLocation>>()
                    .Value;
            }
            catch (Exception ex)
            {
                throw new ConfigurationErrorException("Failed to parse filter location", ex);
            }

            if (filterLocation.EnableExternalFilter)
            {
                if (string.IsNullOrWhiteSpace(filterLocation.FilterImageReference))
                {
                    throw new ConfigurationErrorException($"Filter image reference can not be empty when external filter configuration is enable.");
                }

                ValidateUtility.ValidateImageReference(filterLocation.FilterImageReference);

                if (string.IsNullOrWhiteSpace(filterLocation.FilterConfigurationFileName))
                {
                    throw new ConfigurationErrorException($"Filter configuration file name can not be empty when external filter configuration is enable.");
                }
            }
            else
            {
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

                ValidateUtility.ValidateFilterConfiguration(filterConfiguration);
            }

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
                    ValidateUtility.ValidateImageReference(schemaConfiguration.SchemaImageReference);
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
    }
}
