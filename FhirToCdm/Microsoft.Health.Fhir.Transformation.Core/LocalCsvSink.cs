// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Logging;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public class LocalCsvSink : ISink
    {
        private object _syncRoot = new object();
        private readonly ILogger _logger = TransformationLogging.CreateLogger<LocalCsvSink>();
        private string _folderPath;
        private Dictionary<string, StreamWriter> _writers = new Dictionary<string, StreamWriter>();

        public LocalCsvSink(string folderPath)
        {
            _folderPath = folderPath;
        }

        public Func<string, string> CsvFilePath { get; set; } =
            (string tableName) =>
            {
                return $"{tableName}.csv";
            };

        public async Task InitAsync()
        {
            if (!Directory.Exists(_folderPath))
            {
                Directory.CreateDirectory(_folderPath);
                _logger.LogInformation($"{_folderPath} created.");
            }

            await Task.CompletedTask;
        }

        public async Task CompleteAsync()
        {
            foreach ((string tableName, StreamWriter writer) in _writers)
            {
                await writer.FlushAsync();

                _logger.LogInformation($"{tableName} data flushed.");
                DisposeWriter(writer);
            }
        }

        public async Task WriteAsync(string tableName, string[] columns, Dictionary<string, (object valueObj, object typeObj)> item)
        {
            if (!_writers.ContainsKey(tableName))
            {
                _writers[tableName] = CreateWriteByTableName(tableName);
                await InitializeHeadersAsync(tableName, columns);
            }
            string newLine = CsvUtils.ConvertToCsvRow(columns, item);
            await _writers[tableName].WriteLineAsync(newLine);
        }

        private static void DisposeWriter(StreamWriter writer)
        {
            writer.Close();
            writer.Dispose();
        }

        private StreamWriter CreateWriteByTableName(string tableName)
        {
            string fileName = Path.Combine(_folderPath, CsvFilePath(tableName));
            string directory = Path.GetDirectoryName(fileName);

            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            return new StreamWriter(new FileStream(fileName, FileMode.Create));
        }

        private async Task InitializeHeadersAsync(string tableName, string[] columns)
        { 
            string headers = string.Join(",", columns);
            await _writers[tableName].WriteLineAsync(headers);
        }
    }
}
