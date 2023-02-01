// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.AnalyticsConnector.Common.Authentication;

namespace Microsoft.Health.AnalyticsConnector.Common
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