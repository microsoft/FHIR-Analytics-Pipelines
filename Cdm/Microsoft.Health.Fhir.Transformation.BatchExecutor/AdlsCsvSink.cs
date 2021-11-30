// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Transformation.Core;

namespace Microsoft.Health.Fhir.Transformation.Cdm.BatchExecutor
{
    public class AdlsCsvSink : ISink
    {
        private string _account;
        private ClientSecretCredential _credential;
        private string _fileSystem;
        private DataLakeServiceClient _dataLakeServiceClient;
        private Dictionary<string, StringBuilder> _contentCache;
        private Dictionary<string, int> _offsetRecord;
        private LinkedList<Task<long>> _runningTasks;
        private readonly ILogger _logger = TransformationLogging.CreateLogger<AdlsCsvSink>();


        public AdlsCsvSink(string account, string fileSystem, ClientSecretCredential credential)
        {
            _account = account;
            _fileSystem = fileSystem;
            _credential = credential;

            _contentCache = new Dictionary<string, StringBuilder>();
            _offsetRecord = new Dictionary<string, int>();
            _runningTasks = new LinkedList<Task<long>>();
        }

        public int MaxLengthForSingleAppendBlock
        {
            get;
            set;
        } = 1024 * 40;

        public int ConcurrentCount
        {
            get;
            set;
        } = 3;

        public int RetryCount
        {
            get;
            set;
        } = 3;

        public int OperationTimeoutInSeconds
        {
            get;
            set;
        } = 3 * 60;

        public Func<string, string> CsvFilePath { get; set; } =
            (string tableName) =>
            {
                return $"data/Local{tableName}/partition-data.csv";
            };

        public Func<DataLakeFileClient, MemoryStream, int, Task> AppendContentAsync =
            async (DataLakeFileClient fileClient, MemoryStream dataStream, int startOffset) =>
            {
                await fileClient.AppendAsync(dataStream, offset: startOffset);
            };

        public async Task CompleteAsync()
        {
            foreach ((string tableName, StringBuilder data) in _contentCache)
            {
                if (data.Length > 0)
                {
                    await AddAppendContentTask(tableName, data);
                }
            }

            await Task.WhenAll(_runningTasks.ToArray());

            foreach (string tableName in _contentCache.Keys)
            {
                DataLakeFileSystemClient client = await CreateFileSystemClientIfNotExistAsync();

                string fileName = CsvFilePath(tableName);
                var fileClient = client.GetFileClient(fileName);
                await fileClient.FlushAsync(_offsetRecord[tableName]);
            }
        }

        public Task InitAsync()
        {
            _dataLakeServiceClient = CreateDataLakeServiceClient(_account);
            return Task.CompletedTask;
        }

        public async Task WriteAsync(string tableName, string[] columns, Dictionary<string, (object valueObj, object typeObj)> item)
        {
            if (!_contentCache.ContainsKey(tableName))
            {
                _contentCache[tableName] = new StringBuilder();
                _offsetRecord[tableName] = 0;
                await InitializeHeadersAsync(tableName, columns);
            }

            string contentForNewLine = CsvUtils.ConvertToCsvRow(columns, item);
            _contentCache[tableName].AppendLine(contentForNewLine);

            if (_contentCache[tableName].Length >= MaxLengthForSingleAppendBlock)
            {
                StringBuilder contentStringBuilder = _contentCache[tableName];
                _contentCache[tableName] = new StringBuilder();
                await AddAppendContentTask(tableName, contentStringBuilder);
            }
        }

        public async Task<DataLakeFileSystemClient> CreateFileSystemClientIfNotExistAsync()
        {
            DataLakeFileSystemClient client = _dataLakeServiceClient.GetFileSystemClient(_fileSystem);
            await client.CreateIfNotExistsAsync();
            return client;
        }

        public static async Task<DataLakeDirectoryClient> CreateDirectoryIfNotExistAsync(DataLakeFileSystemClient client, string fileName)
        {
            string directory = Path.GetDirectoryName(fileName);
            DataLakeDirectoryClient directoryClient = client.GetDirectoryClient(directory);
            await directoryClient.CreateIfNotExistsAsync();
            return directoryClient;
        }

        private async Task AddAppendContentTask(string tableName, StringBuilder contentStringBuilder)
        {
            byte[] content = Encoding.UTF8.GetBytes(contentStringBuilder.ToString());
            int startOffset = _offsetRecord[tableName];
            _offsetRecord[tableName] += content.Length;

            await WaitUntilPreviousTaskCompleteAsync();

            _runningTasks.AddLast(AppendContentToAdlsFileAsync(tableName, content, startOffset));
        }

        private async Task WaitUntilPreviousTaskCompleteAsync()
        {
            while (_runningTasks.Count() >= ConcurrentCount)
            {
                await _runningTasks.FirstOrDefault();
                _runningTasks.RemoveFirst();
            }
        }

        private async Task<long> AppendContentToAdlsFileAsync(string tableName, byte[] content, int startOffset)
        {
            DataLakeFileSystemClient client = await CreateFileSystemClientIfNotExistAsync();

            string fileName = CsvFilePath(tableName);
            DataLakeDirectoryClient directoryClient = await CreateDirectoryIfNotExistAsync(client, fileName);

            var fileClient = directoryClient.GetFileClient(Path.GetFileName(fileName));
            await fileClient.CreateIfNotExistsAsync();

            using var dataStream = new MemoryStream(content);
            long fileSize = dataStream.Length;

            return await OperationExecutionHelper.InvokeWithTimeoutRetryAsync(async () =>
            {
                await AppendContentAsync(fileClient, dataStream, startOffset);
                _logger.LogInformation($"{startOffset}:{dataStream.Length} appended.");

                return startOffset;
            }, TimeSpan.FromSeconds(OperationTimeoutInSeconds), RetryCount);
        }

        private async Task InitializeHeadersAsync(string tableName, string[] columns)
        {
            string headers = string.Join(",", columns);
            _contentCache[tableName].AppendLine(headers);

            if (_contentCache[tableName].Length >= MaxLengthForSingleAppendBlock)
            {
                StringBuilder contentStringBuilder = _contentCache[tableName];
                _contentCache[tableName] = new StringBuilder();
                await AddAppendContentTask(tableName, contentStringBuilder);
            }
        }

        private DataLakeServiceClient CreateDataLakeServiceClient(string accountName)
        {
            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            return new DataLakeServiceClient(new Uri(dfsUri), _credential);
        }
    }
}
