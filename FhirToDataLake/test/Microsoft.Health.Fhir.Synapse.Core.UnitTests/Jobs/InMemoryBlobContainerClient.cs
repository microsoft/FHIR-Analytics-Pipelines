// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class InMemoryBlobContainerClient : IAzureBlobContainerClient
    {
        private ConcurrentDictionary<string, Stream> _blobStore = new ();
        private ConcurrentDictionary<string, Tuple<string, DateTimeOffset>> _blobLeaseStore = new ();
        private readonly object _leaseLock = new ();

        public async Task<T> GetValue<T>(string objectName)
        {
            Stream stream = await GetBlobAsync(objectName);
            if (stream == null)
            {
                return default(T);
            }

            stream.Position = 0;
            using StreamReader streamReader = new StreamReader(stream);
            string content = streamReader.ReadToEnd();
            return JsonConvert.DeserializeObject<T>(content);
        }

        public Task<bool> CreateBlobAsync(
            string blobName,
            Stream stream,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_blobStore.TryAdd(blobName, stream));
        }

        public Task<bool> DeleteBlobAsync(
            string blobName,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_blobStore.TryRemove(blobName, out _));
        }

        public Task<Stream> GetBlobAsync(
            string blobName,
            CancellationToken cancellationToken = default)
        {
            Stream value = _blobStore.GetValueOrDefault(blobName);
            if (value == null)
            {
                return Task.FromResult(value);
            }

            MemoryStream valueCopy = new MemoryStream();
            value.CopyTo(valueCopy);
            value.Position = 0;
            valueCopy.Position = 0;
            return Task.FromResult(valueCopy as Stream);
        }

        public Task<bool> ReleaseLeaseAsync(string blobName, string leaseId, CancellationToken cancellationToken = default)
        {
            lock (_leaseLock)
            {
                if (_blobLeaseStore.ContainsKey(blobName) && string.Equals(_blobLeaseStore[blobName].Item1, leaseId))
                {
                    return Task.FromResult(_blobLeaseStore.TryRemove(blobName, out _));
                }

                return Task.FromResult(false);
            }
        }

        public Task<bool> BlobExistsAsync(string blobName, CancellationToken cancellationToken)
        {
            return Task.FromResult(_blobStore.ContainsKey(blobName));
        }

        public Task<IEnumerable<string>> ListBlobsAsync(string blobPrefix, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_blobStore.Keys.ToList().Where(name => name.StartsWith(blobPrefix)));
        }

        public Task<string> UpdateBlobAsync(string blobName, Stream stream, CancellationToken cancellationToken = default)
        {
            _blobStore[blobName] = stream;
            return Task.FromResult(blobName);
        }

        public Task<string> AcquireLeaseAsync(string blobName, string leaseId, TimeSpan timeSpan, CancellationToken cancellationToken = default)
        {
            lock (_leaseLock)
            {
                if (!_blobLeaseStore.ContainsKey(blobName) || _blobLeaseStore[blobName].Item2 < DateTimeOffset.UtcNow)
                {
                    string newLeaseId = Guid.NewGuid().ToString("N");
                    _blobLeaseStore[blobName] = new Tuple<string, DateTimeOffset>(newLeaseId, DateTimeOffset.UtcNow.Add(timeSpan));
                    return Task.FromResult(newLeaseId);
                }

                if (string.Equals(_blobLeaseStore[blobName].Item1, leaseId))
                {
                    return Task.FromResult(leaseId);
                }

                return Task.FromResult<string>(null);
            }
        }

        public Task<string> RenewLeaseAsync(string blobName, string leaseId, CancellationToken cancellationToken = default)
        {
            lock (_leaseLock)
            {
                if (!_blobLeaseStore.ContainsKey(blobName) || !string.Equals(_blobLeaseStore[blobName].Item1, leaseId))
                {
                    throw new Exception("Lease ids don't match!");
                }

                _blobLeaseStore[blobName] = new Tuple<string, DateTimeOffset>(leaseId, DateTimeOffset.UtcNow.Add(TimeSpan.FromSeconds(JobConfigurationConstants.DefaultSchedulerServiceLeaseExpirationInSeconds)));
                return Task.FromResult(leaseId);
            }
        }

        public Task MoveDirectoryAsync(string sourceDirectory, string targetDirectory, CancellationToken cancellationToken = default)
        {
            foreach (string path in _blobStore.Keys)
            {
                if (path.StartsWith(sourceDirectory))
                {
                    string childPath = path.Substring(sourceDirectory.Length);
                    string newPath = $"{targetDirectory}{childPath}";
                    if (_blobStore.Remove(path, out Stream value))
                    {
                        _blobStore[newPath] = value;
                    }
                }
            }

            return Task.FromResult(true);
        }

        public Task DeleteDirectoryIfExistsAsync(string directory, CancellationToken cancellationToken = default)
        {
            foreach (string path in _blobStore.Keys)
            {
                if (path.StartsWith(directory) && !string.Equals(directory, path, StringComparison.OrdinalIgnoreCase))
                {
                    _blobStore.TryRemove(path, out _);
                }
            }

            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<PathItem> ListPathsAsync(string directory, [EnumeratorCancellation]CancellationToken cancellationToken)
        {
            HashSet<string> directorySet = new HashSet<string>();
            List<PathItem> result = new List<PathItem>();
            foreach (string path in _blobStore.Keys)
            {
                if (path.StartsWith(directory) && !string.Equals(directory, path, StringComparison.OrdinalIgnoreCase))
                {
                    PathItem pathItem = DataLakeModelFactory.PathItem(path, false, DateTimeOffset.UtcNow, new ETag("test"), 100, string.Empty, string.Empty, string.Empty);
                    yield return await Task.FromResult(pathItem);

                    string[] pathComponents = path.Split('/');
                    string baseDir = pathComponents[0];
                    int i = 1;
                    while (i < pathComponents.Length - 1)
                    {
                        baseDir += $"/{pathComponents[i]}";
                        i++;

                        if (baseDir.StartsWith(directory)
                            && !string.Equals(baseDir, path, StringComparison.OrdinalIgnoreCase)
                            && !directorySet.Contains(baseDir))
                        {
                            PathItem directoryItem = DataLakeModelFactory.PathItem(baseDir, true, DateTimeOffset.UtcNow, new ETag("test"), 100, string.Empty, string.Empty, string.Empty);
                            yield return await Task.FromResult(directoryItem);
                        }
                    }
                }
            }
        }
    }
}
