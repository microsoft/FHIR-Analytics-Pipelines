// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Microsoft.Health.Fhir.Synapse.Core;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;

namespace Microsoft.Health.Fhir.Synapse.FunctionApp
{
    public class Program
    {
        public static void Main()
        {
            var host = new HostBuilder()
                .ConfigureAppConfiguration((builder) =>
                {
                    builder.AddJsonFile("appsettings.json");
                })
                .ConfigureFunctionsWorkerDefaults((context, builder) =>
                {
                    builder.Services
                        .AddHttpClient()
                        .AddConfiguration(context.Configuration)
                        .AddJobScheduler()
                        .AddDataSource()
                        .AddAzure()
                        .AddSchema();
                })
                .Build();

            host.Run();
        }
    }
}