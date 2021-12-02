// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.DataSerialization.Json;
using Microsoft.Health.Fhir.Synapse.DataSerialization.Parquet;

namespace Microsoft.Health.Fhir.Synapse.DataSerialization
{
    public static class DataSerializationRegistrationExtensions
    {
        public static IServiceCollection AddDataSerialization(
            this IServiceCollection services)
        {
            services.AddSingleton<IJsonDataProcessor, JsonDataProcessor>();
            services.AddSingleton<IColumnDataProcessor, ParquetDataProcessor>();
            services.AddSingleton<IFhirSerializer, FhirSerializer>();

            return services;
        }
    }
}
