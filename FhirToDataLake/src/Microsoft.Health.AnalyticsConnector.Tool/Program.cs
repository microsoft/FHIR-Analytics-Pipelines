// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Health.AnalyticsConnector.Common;
using Microsoft.Health.AnalyticsConnector.Common.Extensions;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.Common.Metrics;
using Microsoft.Health.AnalyticsConnector.Core;
using Microsoft.Health.AnalyticsConnector.DataClient;
using Microsoft.Health.AnalyticsConnector.DataWriter;
using Microsoft.Health.AnalyticsConnector.HealthCheck;
using Microsoft.Health.AnalyticsConnector.JobManagement;
using Microsoft.Health.AnalyticsConnector.SchemaManagement;

namespace Microsoft.Health.AnalyticsConnector.Tool
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            IHost host = CreateHostBuilder(args).Build();
            await host.RunAsync();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                    services
                        .AddConfiguration(context.Configuration)
                        .AddAzure()
                        .AddJobScheduler()
                        .AddJobManagement()
                        .AddDataSource()
                        .AddDataWriter()
                        .AddSchema()
                        .AddHealthCheckService()
                        .AddMetricsLogger()
                        .AddDiagnosticLogger()
                        .AddHostedService<SynapseLinkService>()
                        .AddApplicationInsightsTelemetryWorkerService());
    }
}
