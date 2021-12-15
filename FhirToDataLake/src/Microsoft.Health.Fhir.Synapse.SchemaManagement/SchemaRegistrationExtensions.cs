// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement
{
    public static class SchemaRegistrationExtensions
    {
        public static IServiceCollection AddSchema(this IServiceCollection services)
        {
            services.AddSingleton<IFhirSchemaManager, FhirSchemaManager>();

            return services;
        }
    }
}
