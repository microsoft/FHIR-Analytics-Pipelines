// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public interface ISchedulerService
    {
        /// <summary>
        /// Heartbeat.
        /// </summary>
        public DateTimeOffset LastHeartbeat { get; set; }

        /// <summary>
        /// Run scheduler
        /// </summary>
        /// <param name="cancellationToken">the cancellation token.</param>
        /// <returns>completed job.</returns>
        public Task RunAsync(CancellationToken cancellationToken);
    }
}