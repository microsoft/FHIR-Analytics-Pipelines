// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Core.DataFilter;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core
{
    public static class PipelineRegistrationExtensions
    {
        public static IServiceCollection AddJobScheduler(
            this IServiceCollection services)
        {
            services.AddSingleton<IJobStore, AzureBlobJobStore>();

            services.AddSingleton<JobProgressUpdaterFactory, JobProgressUpdaterFactory>();

            services.AddSingleton<JobManager, JobManager>();

            services.AddSingleton<IJobExecutor, JobExecutor>();

            services.AddSingleton<ITaskExecutor, TaskExecutor>();

            services.AddSingleton<IColumnDataProcessor, ParquetDataProcessor>();

            services.AddSingleton<IFhirSpecificationProvider, R4FhirSpecificationProvider>();

            services.AddSingleton<IGroupMemberExtractor, GroupMemberExtractor>();

            services.AddSingleton<ITypeFilterParser, TypeFilterParser>();

            services.AddSingleton<IReferenceParser, R4ReferenceParser>();

            return services;
        }
    }
}
