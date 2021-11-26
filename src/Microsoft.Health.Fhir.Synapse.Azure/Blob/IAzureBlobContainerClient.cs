// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Azure
{
    public interface IAzureBlobContainerClient
    {
        /// <summary>
        /// Create a new blob. Returns false if the blob already exists.
        /// </summary>
        /// <param name="blobName">The blob name to create.</param>
        /// <param name="stream">blob content.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>whether operation succeeds.</returns>
        public Task<bool> CreateBlobAsync(
            string blobName,
            Stream stream,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Get a blob content.
        /// </summary>
        /// <param name="blobName">blob name.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>blob content.</returns>
        public Task<Stream> GetBlobAsync(
            string blobName,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Delete a blob with an optional leaseId.
        /// </summary>
        /// <param name="blobName">blob name to delete.</param>
        /// <param name="leaseId">lease id, optional.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>whether delete succeeds.</returns>
        public Task<bool> DeleteBlobAsync(
            string blobName,
            string leaseId = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Upload stream to blob.
        /// </summary>
        /// <param name="blobName">blob name.</param>
        /// <param name="stream">data to upload.</param>
        /// <param name="overwrite">overwrite blob if exists.</param>
        /// <param name="leaseId">lease id.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>blob url.</returns>
        public Task<string> UploadBlobAsync(
            string blobName,
            Stream stream,
            bool overwrite = false,
            string leaseId = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Acquire lease for a blob.
        /// </summary>
        /// <param name="blobName">blob name.</param>
        /// <param name="leaseId">lease id.</param>
        /// <param name="timeSpan">time span for lease.</param>
        /// <param name="force">break current lease and acquire new.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Lease id.</returns>
        public Task<string> AcquireLeaseAsync(
            string blobName,
            string leaseId,
            TimeSpan timeSpan,
            bool force = false,
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
