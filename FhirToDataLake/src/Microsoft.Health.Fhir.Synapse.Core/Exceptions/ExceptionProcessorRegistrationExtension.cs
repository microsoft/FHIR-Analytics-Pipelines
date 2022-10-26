// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Core.Exceptions.ErrorProcessors;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions
{
    public static class ExceptionProcessorRegistrationExtension
    {
        public static IServiceCollection AddJobExecutionErrorProcessor(this IServiceCollection services)
        {
            services.AddSingleton<IJobExecutionErrorProcessor, JobExecutionErrorProcessor>();

            return services;
        }
    }
}
