// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;

namespace Microsoft.Health.Fhir.Synapse.Common
{
    public static class AzureDependencyRegistrationExtensions
    {
        public static IServiceCollection AddAzure(this IServiceCollection services)
        {
            services.AddSingleton<ITokenCredentialProvider, DefaultTokenCredentialProvider>();

            return services;
        }

    }
}