// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.Fhir.Synapse.Azure.Blob;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.DataWriter.Blob;

namespace Microsoft.Health.Fhir.Synapse.Azure
{
    public static class DataWriterRegistrationExtensions
    {
        public static IServiceCollection AddAzure(this IServiceCollection services)
        {
            services.AddSingleton<IAzureBlobContainerClientFactory, AzureBlobContainerClientFactory>();
            services.AddSingleton<IDataSink, AzureBlobDataSink>();
            services.AddSingleton<IFhirDataWriter, AzureBlobDataWriter>();

            return services;
        }
    }
}
