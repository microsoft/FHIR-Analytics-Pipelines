// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Files.DataLake.Models;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.AnalyticsConnector.DataClient.Models;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;

namespace Microsoft.Health.AnalyticsConnector.DataClient.DataLake
{
    public class AzureDataLakeClient : IDataLakeClient
    {
        private readonly IAzureBlobContainerClient _containerClient;
        private readonly ILogger<AzureDataLakeClient> _logger;
        private const string _ndjsonSuffix = ".ndjson";

        public AzureDataLakeClient(
            IAzureBlobContainerClientFactory containerClientFactory,
            IDataLakeSource dataLakeDataSource,
            ILogger<AzureDataLakeClient> logger)
        {
            EnsureArg.IsNotNull(containerClientFactory, nameof(containerClientFactory));
            EnsureArg.IsNotNull(dataLakeDataSource, nameof(dataLakeDataSource));

            _containerClient = containerClientFactory.Create(dataLakeDataSource.StorageUrl, dataLakeDataSource.Location);
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
        }

        public async Task<List<AzureBlobInfo>> ListBlobsAsync(BaseDataLakeOptions dataLakeOptions, CancellationToken cancellationToken = default)
        {
            var blobs = await _containerClient.ListBlobInfoAsync(string.Empty, cancellationToken);
            var filteredBlobs = new List<AzureBlobInfo>();

            foreach (var blob in blobs)
            {
                if ((dataLakeOptions.StartTime != null && blob.LastModified < dataLakeOptions.StartTime) ||
                    (dataLakeOptions.EndTime != null && blob.LastModified >= dataLakeOptions.EndTime) ||
                    !blob.Name.ToLower().EndsWith(_ndjsonSuffix))
                {
                    continue;
                }

                filteredBlobs.Add(blob);
            }

            return filteredBlobs;
        }

        public string GetBlobETag(string blobName)
        {
            return _containerClient.GetBlobETag(blobName);
        }

        public async Task<Stream> LoadBlobAsync(string blobName, CancellationToken cancellationToken = default)
        {
            return await _containerClient.DownloadFileAsync(string.Empty, blobName, cancellationToken);
        }

        public async Task<string> AcquireLeaseAsync(string blobName, string leaseId, TimeSpan timeSpan, CancellationToken cancellationToken = default)
        {
            return await _containerClient.AcquireLeaseAsync(blobName, leaseId, timeSpan, cancellationToken);
        }

        public async Task<bool> ReleaseLeaseAsync(string blobName, string leaseId, CancellationToken cancellationToken = default)
        {
            return await _containerClient.ReleaseLeaseAsync(blobName, leaseId, cancellationToken);
        }
    }
}
