// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.AnalyticsConnector.DataClient.Models;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;

namespace Microsoft.Health.AnalyticsConnector.DataClient
{
    public interface IDataLakeClient
    {
        /// <summary>
        /// List data lake blobs that match the options.
        /// </summary>
        /// <param name="dataLakeOptions">data lake options.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>list of azure blob info.</returns>
        public Task<List<AzureBlobInfo>> ListBlobsAsync(
            BaseDataLakeOptions dataLakeOptions,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Get blob ETag.
        /// </summary>
        /// <param name="blobName">blob name.</param>
        /// <returns>blob ETag.</returns>
        public string GetBlobETag(
            string blobName);

        /// <summary>
        /// Load blob in stream.
        /// </summary>
        /// <param name="blobName">blob name.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>blob stream.</returns>
        public Task<Stream> LoadBlobAsync(
            string blobName,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Acquire lease for a blob.
        /// </summary>
        /// <param name="blobName">blob name.</param>
        /// <param name="leaseId">lease id.</param>
        /// <param name="timeSpan">time span for lease.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>lease id if successfully acquired.</returns>
        public Task<string> AcquireLeaseAsync(
            string blobName,
            string leaseId,
            TimeSpan timeSpan,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Release lease for a blob.
        /// </summary>
        /// <param name="blobName">blob name.</param>
        /// <param name="leaseId">lease id.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>operation result.</returns>
        public Task<bool> ReleaseLeaseAsync(
            string blobName,
            string leaseId,
            CancellationToken cancellationToken = default);
    }
}
