// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public interface IJobExecutor
    {
        /// <summary>
        /// Execute job
        /// </summary>
        /// <param name="job">the job.</param>
        /// <param name="cancellationToken">the cancellation token.</param>
        /// <returns>completed job.</returns>
        public Task ExecuteAsync(Job job, CancellationToken cancellationToken);
    }
}
