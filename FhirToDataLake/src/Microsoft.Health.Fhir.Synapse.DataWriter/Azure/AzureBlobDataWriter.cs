// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.Azure
{
    public class AzureBlobDataWriter : IFhirDataWriter
    {
        // Date format in blob path.
        private const string DateKeyFormat = "yyyy/MM/dd";

        private readonly IAzureBlobContainerClient _containerClient;
        private readonly ILogger<AzureBlobDataWriter> _logger;

        // Staged data folder path: "staging/{jobid}/{schemaType}/{year}/{month}/{day}"
        // Committed data file path: "result/{schemaType}/{year}/{month}/{day}/{jobid}"
        private readonly Regex _stagingDataFolderRegex = new Regex(AzureStorageConstants.StagingFolderName + @"/[0-9]{20}/(?<partition>[A-Za-z_]+/\d{4}/\d{2}/\d{2})$");

        // Staged data file path: "staging/{jobid}/{schemaType}/{year}/{month}/{day}/{schemaType}_{jobId}_{partId}.parquet"
        // Committed data file path: "result/{schemaType}/{year}/{month}/{day}/{jobid}/{schemaType}_{jobId}_{partId}.parquet"
        private readonly Regex _stagingDataBlobRegex = new Regex(AzureStorageConstants.StagingFolderName + @"/[a-z0-9]{32}/[A-Za-z_]+/\d{4}/\d{2}/\d{2}/(?<schema>[A-Za-z_]+)_[0-9]{20}_(?<partId>\d+).parquet$");

        public AzureBlobDataWriter(
            IAzureBlobContainerClientFactory containerClientFactory,
            IDataSink dataSink,
            ILogger<AzureBlobDataWriter> logger)
        {
            EnsureArg.IsNotNull(containerClientFactory, nameof(containerClientFactory));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _containerClient = containerClientFactory.Create(dataSink.StorageUrl, dataSink.Location);
            _logger = logger;
        }

        public async Task<string> WriteAsync(
            StreamBatchData data,
            long jobId,
            int partId,
            DateTime dateTime,
            CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(data, nameof(data));

            var schemaType = data.SchemaType;

            var blobName = GetDataFileName(dateTime, schemaType, jobId, partId);
            var blobUrl = await _containerClient.UpdateBlobAsync(blobName, data.Value, cancellationToken);

            _logger.LogInformation($"Write stream batch data to {blobUrl} successfully.");

            return blobUrl;
        }

        private static string GetDataFileName(
            DateTime dateTime,
            string schemaType,
            long jobId,
            int partId)
        {
            var dateTimeKey = dateTime.ToString(DateKeyFormat);

            return $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}/{schemaType}/{dateTimeKey}/{schemaType}_{jobId:d20}_{partId:d6}.parquet";
        }

        public async Task CommitJobDataAsync(long jobId, CancellationToken cancellationToken = default)
        {
            string sourceDirectory = $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}";
            var directoryPairs = new List<Tuple<string, string>>();

            await foreach (var path in _containerClient.ListPathsAsync(sourceDirectory, cancellationToken))
            {
                if (path.IsDirectory == true)
                {
                    // Record all directories that need to commit.
                    var match = _stagingDataFolderRegex.Match(path.Name);
                    if (match.Success)
                    {
                        var destination = $"{AzureStorageConstants.ResultFolderName}/{match.Groups["partition"].Value}/{jobId:d20}";
                        directoryPairs.Add(new Tuple<string, string>(path.Name, destination));
                    }
                }
            }

            // move directories from staging to result folder.
            var moveTasks = new List<Task>();
            foreach (var pair in directoryPairs)
            {
                if (moveTasks.Count >= 10)
                {
                    var completedTask = await Task.WhenAny(moveTasks);
                    await completedTask;
                    moveTasks.Remove(completedTask);
                }

                moveTasks.Add(_containerClient.MoveDirectoryAsync(pair.Item1, pair.Item2, cancellationToken));
            }

            await Task.WhenAll(moveTasks);

            // delete staging folder when success.
            await _containerClient.DeleteDirectoryIfExistsAsync(sourceDirectory, cancellationToken);
        }
    }
}
