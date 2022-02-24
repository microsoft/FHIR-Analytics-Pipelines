// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Tool
{
    public class SynapseLinkService : BackgroundService
    {
        private readonly JobManager _jobManager;
        private readonly IHostApplicationLifetime _hostApplicationLifetime;

        public SynapseLinkService(
            IHostApplicationLifetime hostApplicationLifetime,
            JobManager jobManager)
        {
            _hostApplicationLifetime = hostApplicationLifetime;
            _jobManager = jobManager;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await _jobManager.RunAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                _hostApplicationLifetime.StopApplication();
            }

            Console.WriteLine("Execute finished sucessfully!");
            _hostApplicationLifetime.StopApplication();
        }

        public override Task StopAsync(CancellationToken stoppingToken)
        {
            _jobManager.Dispose();
            return Task.CompletedTask;
        }
    }
}
