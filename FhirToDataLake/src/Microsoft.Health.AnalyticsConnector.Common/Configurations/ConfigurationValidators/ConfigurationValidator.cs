// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using EnsureThat;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;
using NCrontab;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations.ConfigurationValidators
{
    public static class ConfigurationValidator
    {
        /// <summary>
        /// Validate configuration in service collection.
        /// </summary>
        /// <param name="services">Service collection instance.</param>
        /// <exception cref="ConfigurationErrorException">Throw ConfigurationErrorException if configuration is invalid.</exception>
        public static void Validate(IServiceCollection services)
        {
            ValidateDataSourceConfig(services);
            ValidateJobConfig(services);
            ValidateFilterConfig(services);
            ValidateDataLakeStoreConfig(services);
            ValidateSchemaConfig(services);
            ValidateHealthCheckConfig(services);
        }

        private static void ValidateDataSourceConfig(IServiceCollection services)
        {
            DataSourceConfiguration dataSourceConfiguration;
            try
            {
                // include enum field, an exception will be thrown when parse invalid enum string
                // enum field parsing is case insensitive
                dataSourceConfiguration = services
                    .BuildServiceProvider()
                    .GetRequiredService<IOptions<DataSourceConfiguration>>()
                    .Value;
            }
            catch (Exception ex)
            {
                throw new ConfigurationErrorException("Failed to parse data source configuration", ex);
            }

            switch (dataSourceConfiguration.Type)
            {
                case DataSourceType.FHIR:
                    var fhirServerConfiguration = dataSourceConfiguration.FhirServer;

                    if (string.IsNullOrWhiteSpace(fhirServerConfiguration.ServerUrl))
                    {
                        throw new ConfigurationErrorException("Fhir server url can not be empty.");
                    }

                    if (!ConfigurationConstants.SupportedFhirVersions.Contains(fhirServerConfiguration.Version))
                    {
                        throw new ConfigurationErrorException($"Fhir version {fhirServerConfiguration.Version} is not supported.");
                    }

                    break;
                case DataSourceType.DICOM:
                    var dicomServerConfiguration = dataSourceConfiguration.DicomServer;

                    if (string.IsNullOrWhiteSpace(dicomServerConfiguration.ServerUrl))
                    {
                        throw new ConfigurationErrorException("DICOM server url can not be empty.");
                    }

                    if (!ConfigurationConstants.SupportedDicomApiVersions.Contains(dicomServerConfiguration.ApiVersion))
                    {
                        throw new ConfigurationErrorException($"DICOM server API version {dicomServerConfiguration.ApiVersion} is not supported.");
                    }

                    break;
                case DataSourceType.FhirDataLakeStore:
                    var fhirDataLakeStoreConfiguration = dataSourceConfiguration.FhirDataLakeStore;

                    if (string.IsNullOrWhiteSpace(fhirDataLakeStoreConfiguration.StorageUrl))
                    {
                        throw new ConfigurationErrorException("FHIR data lake store storage URL cannot be empty.");
                    }

                    if (string.IsNullOrWhiteSpace(fhirDataLakeStoreConfiguration.ContainerName))
                    {
                        throw new ConfigurationErrorException("FHIR data lake store container name cannot be empty.");
                    }

                    break;
                default:
                    throw new ConfigurationErrorException($"Data source type {dataSourceConfiguration.Type} is not supported");
            }
        }

        private static void ValidateJobConfig(IServiceCollection services)
        {
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
                throw new ConfigurationErrorException("Table Url can not be empty.");
            }

            if (string.IsNullOrWhiteSpace(jobConfiguration.QueueUrl))
            {
                throw new ConfigurationErrorException("Queue Url can not be empty.");
            }

            if (string.IsNullOrWhiteSpace(jobConfiguration.SchedulerCronExpression))
            {
                throw new ConfigurationErrorException("Scheduler crontab expression can not be empty.");
            }

            if (string.IsNullOrWhiteSpace(jobConfiguration.ContainerName))
            {
                throw new ConfigurationErrorException("Target azure container name can not be empty.");
            }

            if (jobConfiguration.MaxRunningJobCount < 1)
            {
                throw new ConfigurationErrorException("Max running job count should be greater than 0.");
            }

            if (jobConfiguration.MaxQueuedJobCountPerOrchestration < 1)
            {
                throw new ConfigurationErrorException("Max queued job count per orchestration job should be greater than 0.");
            }

            if (jobConfiguration.StartTime != null && jobConfiguration.EndTime != null && jobConfiguration.StartTime >= jobConfiguration.EndTime)
            {
                throw new ConfigurationErrorException("The start time should be less than the end time.");
            }

            CrontabSchedule crontabSchedule = CrontabSchedule.TryParse(jobConfiguration.SchedulerCronExpression, new CrontabSchedule.ParseOptions { IncludingSeconds = true });
            if (crontabSchedule == null)
            {
                throw new ConfigurationErrorException("The scheduler crontab expression is invalid.");
            }
        }

        private static void ValidateFilterConfig(IServiceCollection services)
        {
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
                    throw new ConfigurationErrorException("Filter image reference can not be empty when external filter configuration is enable.");
                }

                ValidateUtility.ValidateImageReference(filterLocation.FilterImageReference);
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
        }

        private static void ValidateDataLakeStoreConfig(IServiceCollection services)
        {
            DataLakeStoreConfiguration storeConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<DataLakeStoreConfiguration>>()
                .Value;

            if (string.IsNullOrWhiteSpace(storeConfiguration.StorageUrl))
            {
                throw new ConfigurationErrorException("Target azure storage url can not be empty.");
            }
        }

        private static void ValidateSchemaConfig(IServiceCollection services)
        {
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
                    throw new ConfigurationErrorException("Customized schema image reference can not be empty when customized schema is enable.");
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
                    throw new ConfigurationErrorException("Found the schema image reference but customized schema is disable.");
                }
            }
        }

        private static void ValidateHealthCheckConfig(IServiceCollection services)
        {
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
