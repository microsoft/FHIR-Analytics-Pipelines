// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Health.Fhir.Synapse.Common.Metrics
{
    public static class MetricsLoggerRegistrationExtension
    {
        public static IServiceCollection AddMetricsLogger(this IServiceCollection services)
        {
            services.AddSingleton<IMetricsLogger, MetricsLogger>();

            return services;
        }
    }
}
