// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.DataConverter;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions;
using Microsoft.Health.Fhir.Synapse.Core.Fhir.SpecificationProviders;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.Core
{
    public static class PipelineRegistrationExtensions
    {
        public static IServiceCollection AddJobScheduler(
            this IServiceCollection services)
        {
            services.AddSingleton<JobHosting, JobHosting>();

            services.AddSingleton<IJobFactory, AzureStorageJobFactory>();

            services.AddSingleton<IAzureTableClientFactory, AzureTableClientFactory>();

            services.AddSingleton<JobManager, JobManager>();

            services.AddSingleton<ISchedulerService, SchedulerService>();

            services.AddSingleton<IMetadataStore, AzureTableMetadataStore>();

            services.AddSingleton<IColumnDataProcessor, ParquetDataProcessor>();

            services.AddSingleton<IGroupMemberExtractor, GroupMemberExtractor>();

            services.AddSingleton<IFilterManager, FilterManager>();

            services.AddSingleton<IReferenceParser, R4ReferenceParser>();

            services.AddFhirSpecificationProviders();

            services.AddSchemaConverters();

            return services;
        }

        public static IServiceCollection AddSchemaConverters(this IServiceCollection services)
        {
            services.AddTransient<DefaultSchemaConverter>();
            services.AddTransient<CustomSchemaConverter>();

            services.AddTransient<DataSchemaConverterDelegate>(delegateProvider => name =>
            {
                return name switch
                {
                    FhirParquetSchemaConstants.DefaultSchemaProviderKey => delegateProvider.GetService<DefaultSchemaConverter>(),
                    FhirParquetSchemaConstants.CustomSchemaProviderKey => delegateProvider.GetService<CustomSchemaConverter>(),
                    _ => throw new ParquetDataProcessorException($"Schema delegate name {name} not found when injecting"),
                };
            });

            return services;
        }

        public static IServiceCollection AddFhirSpecificationProviders(this IServiceCollection services)
        {
            services.AddSingleton<IFhirSpecificationProvider, R4FhirSpecificationProvider>();

            services.AddTransient<R4FhirSpecificationProvider>();
            services.AddTransient<R5FhirSpecificationProvider>();

            services.AddTransient<FhirSpecificationProviderDelegate>(delegateProvider => fhirVersion =>
            {
                return fhirVersion switch
                {
                    FhirVersion.R4 => delegateProvider.GetService<R4FhirSpecificationProvider>(),
                    FhirVersion.R5 => delegateProvider.GetService<R5FhirSpecificationProvider>(),
                    _ => throw new FhirSpecificationProviderException($"Fhir version {fhirVersion} is not supported when injecting"),
                };
            });

            return services;
        }
    }
}