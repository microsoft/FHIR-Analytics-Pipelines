// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Health.Fhir.Synapse.Azure;
using Microsoft.Health.Fhir.Synapse.Common.Extensions;
using Microsoft.Health.Fhir.Synapse.DataSerialization;
using Microsoft.Health.Fhir.Synapse.DataSink;
using Microsoft.Health.Fhir.Synapse.DataSource;
using Microsoft.Health.Fhir.Synapse.Scheduler;
using Microsoft.Health.Fhir.Synapse.Scheduler.Jobs;
using Microsoft.Health.Fhir.Synapse.Schema;

namespace Microsoft.Health.Fhir.Synapse.Tool
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();
            await host.RunAsync();
        }

        static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                    services
                        .AddConfiguration(context.Configuration)
                        .AddFhirSpecification()
                        .AddAzure()
                        .AddJobScheduler()
                        .AddDataSource()
                        .AddDataSink()
                        .AddDataSerialization()
                        .AddSchema()
                        .AddHostedService<SynapseLinkService>());
    }
}
