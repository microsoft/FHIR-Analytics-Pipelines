// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Configurations.ConfigurationResolvers;
using Microsoft.Health.AnalyticsConnector.Common.Configurations.ConfigurationValidators;
using Microsoft.Health.AnalyticsConnector.Common.Exceptions;

namespace Microsoft.Health.AnalyticsConnector.Common.Extensions
{
    public static class ConfigurationRegistrationExtensions
    {
        public static IServiceCollection AddConfiguration(
            this IServiceCollection services,
            IConfiguration configuration)
        {
            string configVersionValue = configuration[ConfigurationConstants.ConfigVersionKey];
            Enum.TryParse(configVersionValue, out SupportedConfigVersion configVersion);

            switch (configVersion)
            {
                case SupportedConfigVersion.V1:
                    ConfigurationResolverV1.Resolve(services, configuration);
                    break;
                case SupportedConfigVersion.V2:
                    ConfigurationResolverV2.Resolve(services, configuration);
                    break;
                default:
                    throw new ConfigurationErrorException($"ConfigVersion '{configVersionValue}' is not supported.");
            }

            ConfigurationValidator.Validate(services);

            return services;
        }
    }
}