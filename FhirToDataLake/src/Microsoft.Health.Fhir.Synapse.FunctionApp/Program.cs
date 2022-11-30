// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Metrics;
using Microsoft.Health.Fhir.Synapse.Core;
using Microsoft.Health.Fhir.Synapse.DataClient;
using Microsoft.Health.Fhir.Synapse.DataWriter;
using Microsoft.Health.Fhir.Synapse.JobManagement;
using Microsoft.Health.Fhir.Synapse.SchemaManagement;

namespace Microsoft.Health.Fhir.Synapse.FunctionApp
{
    public class Program
    {
        public static void Main()
        {
            var assembly = Assembly.GetExecutingAssembly();
            Console.WriteLine($"Running FHIR to Synapse Pipeline. Version: {assembly.GetName().Version}");

            IHost host = new HostBuilder()
                .ConfigureAppConfiguration(builder =>
                {
                    builder.AddJsonFile("appsettings.json");
                })
                .ConfigureFunctionsWorkerDefaults((context, builder) =>
                {
                    builder.Services
                        .AddHttpClient()
                        .AddConfiguration(context.Configuration)
                        .AddJobScheduler()
                        .AddJobManagement()
                        .AddDataSource()
                        .AddDataWriter()
                        .AddAzure()
                        .AddSchema()
                        .AddMetricsLogger()
                        .AddDiagnosticLogger();
                })
                .Build();

            host.Run();
        }
    }
}