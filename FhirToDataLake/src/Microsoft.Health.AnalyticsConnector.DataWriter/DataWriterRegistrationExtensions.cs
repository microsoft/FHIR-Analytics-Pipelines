// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;

namespace Microsoft.Health.AnalyticsConnector.DataWriter
{
    public static class DataWriterRegistrationExtensions
    {
        public static IServiceCollection AddDataWriter(this IServiceCollection services)
        {
            services.AddSingleton<IAzureBlobContainerClientFactory, AzureBlobContainerClientFactory>();
            services.AddSingleton<IDataSink, AzureBlobDataSink>();
            services.AddSingleton<IDataWriter, AzureBlobDataWriter>();

            return services;
        }
    }
}
