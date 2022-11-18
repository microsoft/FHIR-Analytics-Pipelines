// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public static class JobManagementRegistrationExtensions
    {
        public static IServiceCollection AddJobManagement(this IServiceCollection services)
        {
            services.AddSingleton<IAzureStorageClientFactory, AzureStorageClientFactory>();

            return services;
        }
    }
}