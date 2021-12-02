// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Common;

namespace Microsoft.Health.Fhir.Synapse.DataSink
{
    public static class DataSinkRegistrationExtensions
    {
        public static IServiceCollection AddDataSink(
            this IServiceCollection services)
        {
            services.AddSingleton<IFhirDataSink, AzureBlobDataSink>();
            services.AddSingleton<IFhirDataWriter, FhirDataWriter>();

            return services;
        }
    }
}
