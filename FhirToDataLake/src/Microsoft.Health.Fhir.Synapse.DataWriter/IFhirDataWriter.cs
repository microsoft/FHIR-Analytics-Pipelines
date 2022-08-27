// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;

namespace Microsoft.Health.Fhir.Synapse.DataWriter
{
    public interface IFhirDataWriter
    {
        /// <summary>
        /// Write stream batch data to storage.
        /// </summary>
        /// <param name="data">The data to be written.</param>
        /// <param name="jobId">The job id.</param>
        /// <param name="partId">The part id.</param>
        /// <param name="dateTime">The dateTime.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The storage url.</returns>
        public Task<string> WriteAsync(
            StreamBatchData data,
            long jobId,
            int partId,
            DateTimeOffset dateTime,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Commit job data to result folder.
        /// </summary>
        /// <param name="jobId">The job id.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Completed job.</returns>
        public Task CommitJobDataAsync(long jobId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Try to clean the staging job data
        /// </summary>
        /// <param name="jobId">The job id.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>Return true if the job data is cleaned, otherwise return false.</returns>
        public Task<bool> TryCleanJobDataAsync(long jobId, CancellationToken cancellationToken = default);
    }
}