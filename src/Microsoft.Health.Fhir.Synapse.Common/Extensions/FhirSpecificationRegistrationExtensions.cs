// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Fhir;

namespace Microsoft.Health.Fhir.Synapse.Common.Extensions
{
    public static class FhirSpecificationRegistrationExtensions
    {
        public static IServiceCollection AddFhirSpecification(
            this IServiceCollection services)
        {
            var fhirServerConfiguration = services
                .BuildServiceProvider()
                .GetRequiredService<IOptions<FhirServerConfiguration>>()
                .Value;

            if (fhirServerConfiguration.Version != FhirVersion.R4)
            {
                throw new ConfigurationErrorException($"Fhir version {fhirServerConfiguration.Version} is not supported.");
            }

            services.AddSingleton<IFhirSpecificationProvider, R4FhirSpecificationProvider>();

            return services;
        }
    }
}
