﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.Azure
{
    public class AzureBlobContainerClient : IAzureBlobContainerClient
    {
        private readonly BlobContainerClient _blobContainerClient;
        private readonly DataLakeFileSystemClient _dataLakeFileSystemClient;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly ILogger<AzureBlobContainerClient> _logger;

        private const int ListBlobPageCount = 20;

        /// <summary>
        /// Path not found error code for data lake api.
        /// See https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/storage/Azure.Storage.Common/src/Shared/Constants.cs#L336.
        /// </summary>
        private const string DataLakePathNotFoundErrorCode = "PathNotFound";

        public AzureBlobContainerClient(
            Uri storageUri,
            ITokenCredentialProvider credentialProvider,
            IDiagnosticLogger diagnosticLogger,
            ILogger<AzureBlobContainerClient> logger)
        {
            EnsureArg.IsNotNull(storageUri, nameof(storageUri));
            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
            EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _diagnosticLogger = diagnosticLogger;
            _logger = logger;

            var externalTokenCredential = credentialProvider.GetCredential(TokenCredentialTypes.External);
            _blobContainerClient = new BlobContainerClient(
                storageUri,
                externalTokenCredential);
            _dataLakeFileSystemClient = new DataLakeFileSystemClient(
                storageUri,
                externalTokenCredential);
            InitializeBlobContainerClient();
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
            _dataLakeFileSystemClient = new DataLakeFileSystemClient(
                connectionString,
                containerName);

            InitializeBlobContainerClient();
        }

        private void InitializeBlobContainerClient()
        {
            try
            {
                _blobContainerClient.CreateIfNotExists();
            }
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogError($"Create container {_blobContainerClient.Name} failed. Reason: {ex}");
                _logger.LogInformation(ex, $"Create container {_blobContainerClient.Name} failed. Reason: {ex}");
                throw new AzureBlobOperationFailedException($"Create container {_blobContainerClient.Name} failed.", ex);
            }
        }

        public async Task<bool> BlobExistsAsync(string blobName, CancellationToken cancellationToken = default)
        {
            var blobClient = _blobContainerClient.GetBlobClient(blobName);
            try
            {
                return await blobClient.ExistsAsync(cancellationToken);
            }
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogError($"Check whether blob '{blobName}' exists failed. Reason: '{ex}'");
                _logger.LogInformation(ex, $"Check whether blob '{blobName}' exists failed. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"Check whether blob '{blobName}' exists failed.", ex);
            }
        }

        public async Task<IEnumerable<string>> ListBlobsAsync(string blobPrefix, CancellationToken cancellationToken = default)
        {
            var blobNameList = new List<string>();
            try
            {
                await foreach (var page in _blobContainerClient.GetBlobsByHierarchyAsync(prefix: blobPrefix, cancellationToken: cancellationToken)
                    .AsPages(default, ListBlobPageCount))
                {
                    blobNameList.AddRange(page.Values.Where(item => item.IsBlob).Select(item => item.Blob.Name));
                }

                return blobNameList;
            }
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogError($"List blob prefix '{blobPrefix}' failed. Reason: '{ex}'");
                _logger.LogInformation(ex, $"List blob prefix '{blobPrefix}' failed. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"List blob prefix '{blobPrefix}' failed.", ex);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"List blob prefix '{blobPrefix}' failed. Reason: '{ex}'");
                _logger.LogError(ex, $"List blob prefix '{blobPrefix}' failed. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"List blob prefix '{blobPrefix}' failed.", ex);
            }
        }

        public async Task<Stream> GetBlobAsync(string blobName, CancellationToken cancellationToken = default)
        {
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
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogError($"Get blob '{blobName}' failed. Reason: '{ex}'");
                _logger.LogInformation(ex, $"Get blob '{blobName}' failed. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"Get blob '{blobName}' failed.", ex);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Get blob '{blobName}' failed. Reason: '{ex}'");
                _logger.LogError(ex, $"Get blob '{blobName}' failed. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"Get blob '{blobName}' failed.", ex);
            }
        }

        public async Task<bool> CreateBlobAsync(string blobName, Stream stream, CancellationToken cancellationToken = default)
        {
            var blobClient = _blobContainerClient.GetBlobClient(blobName);

            stream.Seek(0, SeekOrigin.Begin);
            try
            {
                await blobClient.UploadAsync(stream, cancellationToken);
                _diagnosticLogger.LogInformation($"Created blob '{blobName}' successfully.");
                _logger.LogInformation("Created blob '{0}' successfully.", blobName);

                return true;
            }
            catch (RequestFailedException ex)
                when (ex.ErrorCode == BlobErrorCode.BlobAlreadyExists)
            {
                // if the blob already exists, return false
                _diagnosticLogger.LogInformation($"Blob '{blobName}' already exists.");
                _logger.LogInformation($"Blob '{blobName}' already exists.");
                return false;
            }
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogError($"Create blob '{blobName}' failed. Reason: '{ex}'");
                _logger.LogInformation(ex, $"Create blob '{blobName}' failed. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"Create blob '{blobName}' failed.", ex);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Create blob '{blobName}' failed. Reason: '{ex}'");
                _logger.LogError(ex, $"Create blob '{blobName}' failed. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"Create blob '{blobName}' failed.", ex);
            }
        }

        public async Task<bool> DeleteBlobAsync(string blobName, CancellationToken cancellationToken = default)
        {
            var blob = _blobContainerClient.GetBlobClient(blobName);
            try
            {
                return await blob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: cancellationToken);
            }
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogError($"Delete blob '{blobName}' failed. Reason: '{ex}'");
                _logger.LogInformation(ex, $"Delete blob '{blobName}' failed. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"Delete blob '{blobName}' failed.", ex);
            }
        }

        public async Task<string> UpdateBlobAsync(string blobName, Stream stream, CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(blobName, nameof(blobName));
            EnsureArg.IsNotNull(stream, nameof(stream));

            var blob = _blobContainerClient.GetBlobClient(blobName);

            try
            {
                // will overwrite when blob exists.
                await blob.UploadAsync(stream, true, cancellationToken);
                _diagnosticLogger.LogInformation($"Updated blob '{blobName}' successfully.");
                _logger.LogInformation("Updated blob '{0}' successfully.", blobName);

                return blob.Uri.AbsoluteUri;
            }
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogError($"Update blob '{blobName}' failed. Reason: '{ex}'");
                _logger.LogInformation(ex, $"Update blob '{blobName}' failed. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"Update blob '{blobName}' failed.", ex);
            }
        }

        public async Task<string> AcquireLeaseAsync(string blobName, string leaseId, TimeSpan timeSpan, CancellationToken cancellationToken = default)
        {
            try
            {
                BlobLeaseClient blobLeaseClient = _blobContainerClient.GetBlobClient(blobName).GetBlobLeaseClient(leaseId);
                BlobLease blobLease = await blobLeaseClient.AcquireAsync(timeSpan, cancellationToken: cancellationToken);

                _diagnosticLogger.LogInformation($"Acquire lease on the blob '{blobName}' successfully.");
                _logger.LogInformation("Acquire lease on the blob '{0}' successfully.", blobName);
                return blobLease.LeaseId;
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogInformation($"Failed to acquire lease on the blob '{blobName}'. Reason: '{ex}'");
                _logger.LogInformation("Failed to acquire lease on the blob '{0}'. Reason: '{1}'", blobName, ex);
                return null;
            }
        }

        public async Task<string> RenewLeaseAsync(string blobName, string leaseId, CancellationToken cancellationToken = default)
        {
            try
            {
                var blobClient = _blobContainerClient.GetBlobClient(blobName);
                BlobLeaseClient blobLeaseClient = blobClient.GetBlobLeaseClient(leaseId);
                var leaseResponse = await blobLeaseClient.RenewAsync(cancellationToken: cancellationToken);

                _diagnosticLogger.LogInformation($"Renew lease on the blob '{blobName}' successfully.");
                _logger.LogInformation("Renew lease on the blob '{0}' successfully.", blobName);
                return leaseResponse.Value.LeaseId;
            }
            catch (RequestFailedException ex)
            {
                // When renew lease failed, we should stop the application and exits. So we throw here.
                _diagnosticLogger.LogError($"Failed to renew lease on the blob '{blobName}'. Reason: '{ex}'");
                _logger.LogInformation("Failed to renew lease on the blob '{0}'. Reason: '{1}'", blobName, ex);
                throw new AzureBlobOperationFailedException($"Failed to renew lease on the blob '{blobName}'.", ex);
            }
            catch (Exception ex)
            {
                // When renew lease failed, we should stop the application and exits. So we throw here.
                _diagnosticLogger.LogError($"Failed to renew lease on the blob '{blobName}'. Reason: '{ex}'");
                _logger.LogError("Failed to renew lease on the blob '{0}'. Reason: '{1}'", blobName, ex);
                throw new AzureBlobOperationFailedException($"Failed to renew lease on the blob '{blobName}'.", ex);
            }
        }

        public async Task<bool> ReleaseLeaseAsync(string blobName, string leaseId, CancellationToken cancellationToken = default)
        {
            try
            {
                var blobClient = _blobContainerClient.GetBlobClient(blobName);
                BlobLeaseClient blobLeaseClient = blobClient.GetBlobLeaseClient(leaseId);
                await blobLeaseClient.ReleaseAsync(cancellationToken: cancellationToken);

                _diagnosticLogger.LogInformation($"Release lease on the blob '{blobName}' successfully.");
                _logger.LogInformation("Release lease on the blob '{0}' successfully.", blobName);
                return true;
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogInformation($"Failed to release lease on the blob '{blobName}'. Reason: '{ex}'");
                _logger.LogInformation("Failed to release lease on the blob '{0}'. Reason: '{1}'", blobName, ex);
                return false;
            }
        }

        public async Task MoveDirectoryAsync(string sourceDirectory, string targetDirectory, CancellationToken cancellationToken = default)
        {
            try
            {
                var sourceDirectoryClient = _dataLakeFileSystemClient.GetDirectoryClient(sourceDirectory);

                // Create target parent directory if not exists.
                var targetParentDirectory = Path.GetDirectoryName(targetDirectory);
                if (!string.IsNullOrWhiteSpace(targetParentDirectory))
                {
                    var targetParentDirectoryClient = _dataLakeFileSystemClient.GetDirectoryClient(targetParentDirectory);
                    await targetParentDirectoryClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
                }

                await sourceDirectoryClient.RenameAsync(targetDirectory, cancellationToken: cancellationToken);

                _logger.LogInformation("Move blob directory '{0}' to '{1}' successfully.", sourceDirectory, targetDirectory);
            }
            catch (Exception ex)
            {
                if (ex is RequestFailedException || ex is ArgumentException || ex is PathTooLongException)
                {
                    _logger.LogInformation(ex, "Failed to move blob directory '{0}' to '{1}'. Reason: '{2}'", sourceDirectory, targetDirectory, ex);
                }
                else
                {
                    _logger.LogError(ex, "Failed to move blob directory '{0}' to '{1}'. Reason: '{2}'", sourceDirectory, targetDirectory, ex);
                }

                _diagnosticLogger.LogError($"Failed to move blob directory '{sourceDirectory}' to '{targetDirectory}'. Reason: '{ex}'");
                throw new AzureBlobOperationFailedException($"Failed to move blob directory '{sourceDirectory}' to '{targetDirectory}'.", ex);
            }
        }

        public async Task DeleteDirectoryIfExistsAsync(string directory, CancellationToken cancellationToken = default)
        {
            try
            {
                var directoryClient = _dataLakeFileSystemClient.GetDirectoryClient(directory);
                await directoryClient.DeleteIfExistsAsync(cancellationToken: cancellationToken);

                _diagnosticLogger.LogInformation("Delete blob directory '{directory}' successfully.");
                _logger.LogInformation("Delete blob directory '{0}' successfully.", directory);
            }
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogWarning($"Failed to delete blob directory '{directory}'. Reason: '{ex}'");
                _logger.LogInformation("Failed to delete blob directory '{0}'. Reason: '{1}'", directory, ex);
                throw new AzureBlobOperationFailedException($"Failed to delete blob directory '{directory}'.", ex);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogWarning($"Failed to delete blob directory '{directory}'. Reason: '{ex}'");
                _logger.LogInformation("Failed to delete blob directory '{0}'. Reason: '{1}'", directory, ex);
                throw new AzureBlobOperationFailedException($"Failed to delete blob directory '{directory}'.", ex);
            }
        }

        public async IAsyncEnumerable<PathItem> ListPathsAsync(string directory, [EnumeratorCancellation]CancellationToken cancellationToken = default)
        {
            try
            {
                // Enumerate all paths in folder, see List directory contents https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-dotnet#list-directory-contents
                var directoryClient = _dataLakeFileSystemClient.GetDirectoryClient(directory);
                IAsyncEnumerator<PathItem> asyncEnumerator = directoryClient.GetPathsAsync(true, cancellationToken: cancellationToken).GetAsyncEnumerator(cancellationToken);

                PathItem item;
                do
                {
                    // Exception might throw when querying data lake storage.
                    if (!await asyncEnumerator.MoveNextAsync())
                    {
                        break;
                    }

                    item = asyncEnumerator.Current;
                }
                while (item != null);
            }
            catch (RequestFailedException ex)
                    when (ex.ErrorCode == DataLakePathNotFoundErrorCode)
            {
                _diagnosticLogger.LogInformation($"Directory '{directory}' is empty.");
                _logger.LogInformation("Directory '{0}' is empty.", directory);
                yield break;
            }
            catch (RequestFailedException ex)
            {
                _diagnosticLogger.LogError($"Failed to query paths of directory '{directory}'. Reason: '{ex}'");
                _logger.LogInformation("Failed to query paths of directory '{0}'. Reason: '{1}'", directory, ex);
                throw new AzureBlobOperationFailedException($"Failed to query paths of directory '{directory}'.", ex);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Failed to query paths of directory '{directory}'. Reason: '{ex}'");
                _logger.LogError("Failed to query paths of directory '{0}'. Reason: '{1}'", directory, ex);
                throw new AzureBlobOperationFailedException($"Failed to query paths of directory '{directory}'.", ex);
            }
        }
    }
}
