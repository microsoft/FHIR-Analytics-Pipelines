// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public class LocalNdjsonSource : ISource
    {
        private string _folderName;
        private Queue<Lazy<StreamReader>> _readers = new Queue<Lazy<StreamReader>>();
        private readonly ILogger _logger = TransformationLogging.CreateLogger<LocalNdjsonSource>();

        public LocalNdjsonSource(string folderName)
        {
            _folderName = folderName;
        }

        public async Task CloseAsync()
        {
            foreach (var reader in _readers)
            {
                DisposeReader(reader.Value);
            }

            await Task.CompletedTask;
        }

        public Task OpenAsync()
        {
            foreach (string file in Directory.EnumerateFiles(_folderName, "*.ndjson"))
            {
                _readers.Enqueue(new Lazy<StreamReader>(() =>
                {
                    _logger.LogInformation($"Open file: {file}");
                    return new StreamReader(new FileStream(file, FileMode.Open));
                }));
            }

            return Task.CompletedTask;
        }

        public async Task<string> ReadAsync()
        {
            while (_readers.Count() > 0)
            {
                string content = await _readers.Peek().Value.ReadLineAsync();
                if (string.IsNullOrEmpty(content))
                {
                    DisposeReader(_readers.Dequeue().Value);
                    continue;
                }

                return content;
            }

            return null;
        }

        private static void DisposeReader(StreamReader reader)
        {
            reader?.Close();
            reader?.Dispose();
        }
    }
}
