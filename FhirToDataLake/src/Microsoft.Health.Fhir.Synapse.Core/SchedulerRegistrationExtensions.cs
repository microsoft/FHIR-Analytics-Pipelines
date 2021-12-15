// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.Json;
using Microsoft.Health.Fhir.Synapse.Core.DataProcessor.Parquet;
using Microsoft.Health.Fhir.Synapse.Core.DataWriter;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core
{
    public static class SchedulerRegistrationExtensions
    {
        public static IServiceCollection AddJobScheduler(
            this IServiceCollection services)
        {
            services.AddSingleton<IJobStore, AzureBlobJobStore>();

            services.AddSingleton<JobManager, JobManager>();

            services.AddSingleton<ITaskExecutor, TaskExecutor>();

            services.AddSingleton<IFhirDataWriter, FhirDataWriter>();

            services.AddSingleton<IJsonDataProcessor, JsonDataProcessor>();

            services.AddSingleton<IColumnDataProcessor, ParquetDataProcessor>();

            return services;
        }
    }
}
