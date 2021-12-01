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
            services.AddSingleton<IFhirSpecificationProvider, R4FhirSpecificationProvider>();

            return services;
        }
    }
}
