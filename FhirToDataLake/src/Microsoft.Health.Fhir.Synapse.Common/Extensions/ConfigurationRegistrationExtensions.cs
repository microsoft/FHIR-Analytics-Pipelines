// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.ConfigurationResolvers;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.ConfigurationValidators;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.Common.Extensions
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
                    ConfigurationValidatorV1.Validate(services);
                    break;

                default:
                    throw new ConfigurationErrorException($"ConfigVersion '{configVersionValue}' is not supported.");
            }

            return services;
        }
    }
}