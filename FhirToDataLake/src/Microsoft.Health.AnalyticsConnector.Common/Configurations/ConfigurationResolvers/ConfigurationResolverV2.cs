// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations.ConfigurationResolvers
{
    public class ConfigurationResolverV2 : BaseConfigurationResolver
    {
        public static void Resolve(
            IServiceCollection services,
            IConfiguration configuration)
        {
            services.Configure<DataSourceConfiguration>(options =>
                configuration.GetSection(ConfigurationConstants.DataSourceConfigurationKey).Bind(options));

            BaseResolve(services, configuration);
        }
    }
}
