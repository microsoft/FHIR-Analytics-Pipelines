// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Azure.Authentication;
using Microsoft.Health.Fhir.Synapse.Azure.Blob;

namespace Microsoft.Health.Fhir.Synapse.Azure
{
    public static class AzureRegistrationExtensions
    {
        public static IServiceCollection AddAzure(this IServiceCollection services)
        {
            services.AddSingleton<IAccessTokenProvider, AzureAccessTokenProvider>();
            services.AddSingleton<IAzureBlobContainerClientFactory, AzureBlobContainerClientFactory>();

            return services;
        }
    }
}
