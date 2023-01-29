// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.AnalyticsConnector.Common.Configurations.Arrow;

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations.ConfigurationResolvers
{
    public class ConfigurationResolverV1 : BaseConfigurationResolver
    {
        public static void Resolve(
            IServiceCollection services,
            IConfiguration configuration)
        {
            // Find FhirServerConfiguration instead and create a DataSourceConfiguration from it for backward compatibility.
            services.Configure<DataSourceConfiguration>(options =>
            {
                options.Type = DataSourceType.FHIR;
                configuration.GetSection(ConfigurationConstants.FhirServerConfigurationKey).Bind(options.FhirServer);
            });

            BaseResolve(services, configuration);
        }
    }
}
