// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Scheduler.Jobs;
using Microsoft.Health.Fhir.Synapse.Scheduler.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Scheduler
{
    public static class SchedulerRegistrationExtensions
    {
        public static IServiceCollection AddJobScheduler(
            this IServiceCollection services)
        {
            services.AddSingleton<IJobStore, JobStore>();

            services.AddSingleton<JobManager, JobManager>();

            services.AddSingleton<ITaskExecutor, TaskExecutor>();

            return services;
        }
    }
}
