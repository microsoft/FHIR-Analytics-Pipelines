// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public static class JobManagementRegistrationExtensions
    {
        // TODO: need to add it when generic task is enabled
        public static IServiceCollection AddJobManagement(this IServiceCollection services)
        {
            services.AddSingleton<IAzureStorageClientFactory, AzureStorageClientFactory>();
            services.AddSingleton<IQueueClient, AzureStorageJobQueueClient<FhirToDataLakeAzureStorageJobInfo>>();

            return services;
        }
    }
}