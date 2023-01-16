// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.JobManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.Core
{
    public static class PipelineRegistrationExtensions
    {
        public static IServiceCollection AddJobScheduler(
            this IServiceCollection services)
        {
            var dataSourceConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<DataSourceConfiguration>>()
                .Value;

            switch (dataSourceConfiguration.Type)
            {
                case DataSourceType.FHIR:
                    services.AddSingleton<IJobFactory, FhirAzureStorageJobFactory>();

                    services.AddSingleton<ISchedulerService, FhirSchedulerService>();

                    services.AddSingleton<IGroupMemberExtractor, GroupMemberExtractor>();

                    FilterLocation filterLocation = services
                        .BuildServiceProvider()
                        .GetRequiredService<IOptions<FilterLocation>>()
                        .Value;

                    if (filterLocation.EnableExternalFilter)
                    {
                        services.AddSingleton<IFilterProvider, ContainerRegistryFilterProvider>();
                    }
                    else
                    {
                        services.AddSingleton<IFilterProvider, LocalFilterProvider>();
                    }

                    services.AddSingleton<IFilterManager, FilterManager>();

                    services.AddSingleton<IReferenceParser, R4ReferenceParser>();

                    services.AddFhirSpecificationProvider();

                    services.AddSingleton<IQueueClient, AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo>>();

                    break;
                case DataSourceType.DICOM:
                    services.AddSingleton<IJobFactory, DicomAzureStorageJobFactory>();

                    services.AddSingleton<ISchedulerService, DicomSchedulerService>();

                    services.AddSingleton<IQueueClient, AzureStorageJobQueueClient<DicomToDataLakeAzureStorageJobInfo>>();
                    break;
                default:
                    throw new ConfigurationErrorException($"Data source type {dataSourceConfiguration.Type} is not supported");
            }

            services.AddSingleton<JobHosting, JobHosting>();

            services.AddSingleton<JobManager, JobManager>();

            services.AddSingleton<IAzureTableClientFactory, AzureTableClientFactory>();

            services.AddSingleton<IMetadataStore, AzureTableMetadataStore>();

            services.AddSingleton<IColumnDataProcessor, ParquetDataProcessor>();

            services.AddSingleton<IExternalDependencyChecker, ExternalDependencyChecker>();

            services.AddSchemaConverters(dataSourceConfiguration.Type);

            return services;
        }

        public static IServiceCollection AddSchemaConverters(this IServiceCollection services, DataSourceType dataSourceType)
        {
            switch (dataSourceType)
            {
                case DataSourceType.FHIR:
                    services.AddSingleton<FhirDefaultSchemaConverter>();
                    break;
                case DataSourceType.DICOM:
                    services.AddSingleton<DicomDefaultSchemaConverter>();
                    break;
                default:
                    throw new ConfigurationErrorException($"Data source type {dataSourceType} is not supported");
            }

            services.AddSingleton<CustomSchemaConverter>();

            services.AddSingleton<DataSchemaConverterDelegate>(delegateProvider => name =>
            {
                return name switch
                {
                    ParquetSchemaConstants.DefaultSchemaProviderKey => dataSourceType == DataSourceType.FHIR
                        ? delegateProvider.GetService<FhirDefaultSchemaConverter>()
                        : delegateProvider.GetService<DicomDefaultSchemaConverter>(),
                    ParquetSchemaConstants.CustomSchemaProviderKey => delegateProvider.GetService<CustomSchemaConverter>(),
                    _ => throw new ParquetDataProcessorException($"Schema delegate name {name} not found when injecting"),
                };
            });

            return services;
        }

        public static IServiceCollection AddFhirSpecificationProvider(this IServiceCollection services)
        {
            var dataSourceConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<DataSourceConfiguration>>()
                .Value;

            var fhirServerConfiguration = dataSourceConfiguration.FhirServer;

            switch (fhirServerConfiguration.Version)
            {
                case FhirVersion.R4:
                    services.AddSingleton<IFhirSpecificationProvider, R4FhirSpecificationProvider>(); break;
                case FhirVersion.R5:
                    services.AddSingleton<IFhirSpecificationProvider, R5FhirSpecificationProvider>(); break;
                default:
                    throw new FhirSpecificationProviderException($"Fhir version {fhirServerConfiguration.Version} is not supported when injecting");
            }

            return services;
        }
    }
}