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
        /// <param name="taskIndex">The task index.</param>
        /// <param name="partId">The part id.</param>
        /// <param name="dateTime">The dateTime.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>the storage url.</returns>
        public Task<string> WriteAsync(
            StreamBatchData data,
            string jobId,
            int taskIndex,
            int partId,
            DateTimeOffset dateTime,
            CancellationToken cancellationToken = default);
    }
}
