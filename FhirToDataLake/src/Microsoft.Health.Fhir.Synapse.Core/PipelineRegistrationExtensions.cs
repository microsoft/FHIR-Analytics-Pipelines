// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using AzureTableTaskQueue;
using AzureTableTaskQueue.Synapse;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor;
using Microsoft.Health.Fhir.Synapse.Core.Fhir;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.Core
{
    public static class PipelineRegistrationExtensions
    {
        public static IServiceCollection AddJobScheduler(
            this IServiceCollection services)
        {
            services.AddSingleton<IQueueClient, AzureStorageQueueClient>();
            services.AddSingleton<IJobFactory, AzureStorageJobFactory>();

            services.AddSingleton<SchedulerService, SchedulerService>();
            services.AddSingleton<JobHosting, JobHosting>();
            services.AddSingleton<JobManager, JobManager>();

            services.AddSingleton<IColumnDataProcessor, ParquetDataProcessor>();
            services.AddSingleton<IFhirSpecificationProvider, R4FhirSpecificationProvider>();

            return services;
        }
    }
}
