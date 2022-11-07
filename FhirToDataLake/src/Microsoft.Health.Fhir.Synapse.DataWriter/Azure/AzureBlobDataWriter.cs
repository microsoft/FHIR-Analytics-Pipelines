// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Files.DataLake.Models;
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

        // Concurrency to control how many folders are processed concurrently.
        private const int StageFolderConcurrency = 20;

        // Staged data folder path: "staging/{JobId:d20}/{schemaType}/{year}/{month}/{day}"
        // Committed data file path: "result/{schemaType}/{year}/{month}/{day}/{JobId:d20}"
        private readonly Regex _stagingDataFolderRegex = new Regex(AzureStorageConstants.StagingFolderName + @"/[0-9]{20}/(?<partition>[A-Za-z_]+/\d{4}/\d{2}/\d{2})$");

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
            DateTimeOffset dateTime,
            CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(data, nameof(data));

            string schemaType = data.SchemaType;

            string blobName = GetDataFileName(dateTime, schemaType, jobId, partId);
            string blobUrl = await _containerClient.UpdateBlobAsync(blobName, data.Value, cancellationToken);

            _logger.LogInformation($"Write stream batch data to {blobUrl} successfully.");

            return blobUrl;
        }

        // We first get all pathItems from staging folder.
        // The result contains all subfolders and blobs like:
        //      "staging/jobId" directory
        //      "staging/jobId/schemaType" directory
        //      "staging/jobId/schemaType/year" directory
        //      "staging/jobId/schemaType/year/month" directory
        //      "staging/jobId/schemaType/year/month/day1" directory
        //      "staging/jobId/schemaType/year/month/day1/schemaType_0000000001.parquet" blob
        //      "staging/jobId/schemaType/year/month/day1/schemaType_0000000002.parquet" blob
        //      "staging/jobId/schemaType/year/month/day2" directory
        //      "staging/jobId/schemaType/year/month/day2/schemaType_0000000001.parquet" blob
        //      "staging/jobId/schemaType/year/month/day2/schemaType_0000000002.parquet" blob
        // For directories, we need to find all leaf directory and map to the target directory in result folder.
        // Then rename the source directory to target directory.
        public async Task CommitJobDataAsync(long jobId, CancellationToken cancellationToken = default)
        {
            string sourceDirectory = $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}";
            List<Tuple<string, string>> directoryPairs = new List<Tuple<string, string>>();

            await foreach (PathItem path in _containerClient.ListPathsAsync(sourceDirectory, cancellationToken))
            {
                if (path.IsDirectory == true)
                {
                    // Record all directories that need to commit.
                    Match match = _stagingDataFolderRegex.Match(path.Name);
                    if (match.Success)
                    {
                        string destination = $"{AzureStorageConstants.ResultFolderName}/{match.Groups["partition"].Value}/{jobId:d20}";
                        directoryPairs.Add(new Tuple<string, string>(path.Name, destination));
                    }
                }
            }

            // move directories from staging to result folder.
            List<Task> moveTasks = new List<Task>();
            foreach (Tuple<string, string> pair in directoryPairs)
            {
                if (moveTasks.Count >= StageFolderConcurrency)
                {
                    Task completedTask = await Task.WhenAny(moveTasks);
                    await completedTask;
                    moveTasks.Remove(completedTask);
                }

                moveTasks.Add(_containerClient.MoveDirectoryAsync(pair.Item1, pair.Item2, cancellationToken));
            }

            await Task.WhenAll(moveTasks);

            // delete staging folder when success.
            await TryCleanJobDataAsync(jobId, cancellationToken);
        }

        public async Task<bool> TryCleanJobDataAsync(long jobId, CancellationToken cancellationToken = default)
        {
            string sourceDirectory = $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}";
            try
            {
                await _containerClient.DeleteDirectoryIfExistsAsync(sourceDirectory, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Fail to delete job data from staging folder");
                return false;
            }

            return true;
        }

        private static string GetDataFileName(
            DateTimeOffset dateTime,
            string schemaType,
            long jobId,
            int partId)
        {
            string dateTimeKey = dateTime.ToString(DateKeyFormat);

            return $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}/{schemaType}/{dateTimeKey}/{schemaType}_{partId:d10}.parquet";
        }
    }
}