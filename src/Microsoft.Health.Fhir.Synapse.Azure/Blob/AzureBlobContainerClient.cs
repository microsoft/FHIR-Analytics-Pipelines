// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Azure.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.Azure
{
    public class AzureBlobContainerClient : IAzureBlobContainerClient
    {
        private readonly BlobContainerClient _blobContainerClient;
        private readonly ILogger<AzureBlobContainerClient> _logger;

        public AzureBlobContainerClient(
            Uri storageUri,
            ILogger<AzureBlobContainerClient> logger)
        {
            EnsureArg.IsNotNull(storageUri, nameof(storageUri));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _logger = logger;

            _blobContainerClient = new BlobContainerClient(
                storageUri,
                new DefaultAzureCredential());
            InitializeBlobContainerClient(_blobContainerClient);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureBlobContainerClient"/> class for local tests
        /// since MI auth is not supported for local emulator.
        /// </summary>
        /// <param name="connectionString">Storage connection string.</param>
        /// <param name="containerName">Container name.</param>
        /// <param name="logger">logger.</param>
        public AzureBlobContainerClient(
            string connectionString,
            string containerName,
            ILogger<AzureBlobContainerClient> logger)
        {
            EnsureArg.IsNotNull(connectionString, nameof(connectionString));
            EnsureArg.IsNotNull(containerName, nameof(containerName));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _logger = logger;

            _blobContainerClient = new BlobContainerClient(
                connectionString,
                containerName);
            InitializeBlobContainerClient(_blobContainerClient);
        }

        private void InitializeBlobContainerClient(BlobContainerClient containerClient)
        {
            try
            {
                _blobContainerClient.CreateIfNotExists();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Create container {_blobContainerClient.Name} failed. Reason: {ex}");
                throw new AzureBlobOperationFailedException($"Create container {_blobContainerClient.Name} failed.", ex);
            }
        }

        private async Task EnsureContainerClient(CancellationToken cancellationToken = default)
        {
            try
            {

                if (!await _blobContainerClient.ExistsAsync())
                {
                    _ = await _blobContainerClient.CreateAsync(cancellationToken: cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Create container {_blobContainerClient.Name} failed. Reason: {ex}");
                throw new AzureBlobOperationFailedException($"Create container {_blobContainerClient.Name} failed.", ex);
            }
        }

        public async Task<bool> CreateBlobAsync(string blobName, Stream stream, CancellationToken cancellationToken = default)
        {
            await EnsureContainerClient(cancellationToken);

            var blobClient = _blobContainerClient.GetBlobClient(blobName);

            stream.Seek(0, SeekOrigin.Begin);
            try
            {
                await blobClient.UploadAsync(stream, cancellationToken);
                return true;
            }
            catch (RequestFailedException ex)
                when (ex.ErrorCode == BlobErrorCode.BlobAlreadyExists)
            {
                // if the blob already exists, return false
                _logger.LogInformation($"Blob {blobName} already exists.");
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Create blob {blobName} failed. Reason: {ex}");
                throw new AzureBlobOperationFailedException($"Create blob {blobName} failed.", ex);
            }
        }

        public async Task<bool> DeleteBlobAsync(string blobName, string leaseId = null, CancellationToken cancellationToken = default)
        {
            await EnsureContainerClient(cancellationToken);

            BlobRequestConditions conditions = null;
            if (!string.IsNullOrEmpty(leaseId))
            {
                conditions = new BlobRequestConditions { LeaseId = leaseId };
            }

            var blob = _blobContainerClient.GetBlobClient(blobName);
            try
            {
                return await blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, conditions, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Delete blob {blobName} failed. Reason: {ex}");
                throw new AzureBlobOperationFailedException($"Delete blob {blobName} failed.", ex);
            }
        }

        public async Task<Stream> GetBlobAsync(string blobName, CancellationToken cancellationToken = default)
        {
            await EnsureContainerClient(cancellationToken);

            var blobClient = _blobContainerClient.GetBlobClient(blobName);
            try
            {
                var stream = new MemoryStream();
                await blobClient.DownloadToAsync(stream, cancellationToken);
                stream.Seek(0, SeekOrigin.Begin);
                return stream;
            }
            catch (RequestFailedException ex)
                when (ex.ErrorCode == BlobErrorCode.BlobNotFound)
            {
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Get blob {blobName} failed. Reason: {ex}");
                throw new AzureBlobOperationFailedException($"Get blob {blobName} failed.", ex);
            }
        }

        public async Task<string> UploadBlobAsync(string blobName, Stream stream, bool overwrite = false, string leaseId = null, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(blobName, nameof(blobName));
            EnsureArg.IsNotNull(stream, nameof(stream));

            await EnsureContainerClient(cancellationToken);

            var blob = _blobContainerClient.GetBlobClient(blobName);
            stream.Seek(0, SeekOrigin.Begin);
            if (overwrite)
            {
                BlobUploadOptions blobUploadOptions = new BlobUploadOptions();
                if (!string.IsNullOrEmpty(leaseId))
                {
                    blobUploadOptions.Conditions = new BlobRequestConditions
                    {
                        LeaseId = leaseId,
                    };
                }

                try
                {
                    // will overwrite when blob exists.
                    await blob.UploadAsync(stream, blobUploadOptions, cancellationToken);
                    return blob.Uri.AbsoluteUri;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Upload blob {blobName} failed. Reason: {ex}");
                    throw new AzureBlobOperationFailedException($"Get blob {blobName} failed.", ex);
                }
            }
            else
            {
                try
                {
                    await blob.UploadAsync(stream, cancellationToken);
                    return blob.Uri.AbsoluteUri;
                }
                catch (Exception ex)
                {
                    // if the blob already exists and overwrite is false, return null
                    _logger.LogError(ex, $"Upload blob {blobName} failed. Reason: {ex}");
                    throw new AzureBlobOperationFailedException($"Upload blob {blobName} failed.", ex);
                }
            }
        }

        public async Task<string> AcquireLeaseAsync(string blobName, string leaseId, TimeSpan timeSpan, bool force = false, CancellationToken cancellationToken = default)
        {
            await EnsureContainerClient(cancellationToken);

            try
            {
                BlobLeaseClient blobLeaseClient = _blobContainerClient.GetBlobClient(blobName).GetBlobLeaseClient(leaseId);
                if (force)
                {
                    await blobLeaseClient.BreakAsync(cancellationToken: cancellationToken);
                }

                BlobLease blobLease = await blobLeaseClient.AcquireAsync(timeSpan, cancellationToken: cancellationToken);

                _logger.LogInformation("Acquire lease on the blob {0} successfully.", blobName);
                return blobLease.LeaseId;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to acquire lease on the blob {0}. Reason: {1}", blobName, ex);
                return null;
            }
        }

        public async Task<bool> ReleaseLeaseAsync(string blobName, string leaseId, CancellationToken cancellationToken = default)
        {
            await EnsureContainerClient(cancellationToken);

            try
            {
                var blobClient = _blobContainerClient.GetBlobClient(blobName);
                BlobLeaseClient blobLeaseClient = blobClient.GetBlobLeaseClient(leaseId);
                await blobLeaseClient.ReleaseAsync(cancellationToken: cancellationToken);

                _logger.LogInformation("Release lease on the blob {0} successfully.", blobName);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Failed to release lease on the blob {0}. Reason: {1}", blobName, ex);
                return false;
            }
        }
    }
}
