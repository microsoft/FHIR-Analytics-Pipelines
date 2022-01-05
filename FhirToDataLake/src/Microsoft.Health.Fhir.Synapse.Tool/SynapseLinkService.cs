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
        private JobExecutor _jobExecutor;
        private IHostApplicationLifetime _hostApplicationLifetime;

        public SynapseLinkService(
            IHostApplicationLifetime hostApplicationLifetime,
            JobExecutor jobExecutor)
        {
            _hostApplicationLifetime = hostApplicationLifetime;
            _jobExecutor = jobExecutor;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await _jobExecutor.TriggerJobAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                _hostApplicationLifetime.StopApplication();
            }

            Console.WriteLine("Execute finished sucessfully!");
            _hostApplicationLifetime.StopApplication();
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await _jobExecutor.DisposeAsync();
            Console.WriteLine("Jobmanager gracefully disposed.");
        }
    }
}
