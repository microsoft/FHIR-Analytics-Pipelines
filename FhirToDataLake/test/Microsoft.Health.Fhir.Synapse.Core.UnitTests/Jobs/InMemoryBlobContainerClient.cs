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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class InMemoryBlobContainerClient : IAzureBlobContainerClient
    {
        private ConcurrentDictionary<string, Stream> _blobStore = new ConcurrentDictionary<string, Stream>();
        private ConcurrentDictionary<string, Tuple<string, DateTimeOffset>> _blobLeaseStore = new ConcurrentDictionary<string, Tuple<string, DateTimeOffset>>();
        private readonly object _leaseLock = new object();

        public async Task<T> GetValue<T>(string objectName)
        {
            var stream = await GetBlobAsync(objectName);
            if (stream == null)
            {
                return default(T);
            }

            stream.Position = 0;
            using var streamReader = new StreamReader(stream);
            var content = streamReader.ReadToEnd();
            return JsonConvert.DeserializeObject<T>(content);
        }

        public async Task CreateJob(Job job, string leaseId = null)
        {
            var schedulerSetting = new SchedulerMetadata
            {
                LastScheduledTimestamp = job.DataPeriod.End,
            };
            var jobConfigContent = JsonConvert.SerializeObject(schedulerSetting);
            var configStream = new MemoryStream(Encoding.UTF8.GetBytes(jobConfigContent));
            await CreateBlobAsync(AzureBlobJobConstants.SchedulerMetadataFileName, configStream);

            var jobContent = JsonConvert.SerializeObject(job);
            var jobName = $"{AzureBlobJobConstants.ActiveJobFolder}/{job.Id}.json";
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(jobContent));
            await CreateBlobAsync(jobName, stream);

            if (!string.IsNullOrEmpty(leaseId))
            {
                await CreateBlobAsync(AzureBlobJobConstants.JobLockFileName, new MemoryStream());

                _blobLeaseStore[AzureBlobJobConstants.JobLockFileName] = new Tuple<string, DateTimeOffset>(leaseId, DateTimeOffset.UtcNow.AddSeconds(30));
            }
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
            var value = _blobStore.GetValueOrDefault(blobName);
            if (value == null)
            {
                return Task.FromResult(value);
            }

            var valueCopy = new MemoryStream();
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

                _blobLeaseStore[blobName] = new Tuple<string, DateTimeOffset>(leaseId, DateTimeOffset.UtcNow.Add(TimeSpan.FromSeconds(AzureBlobJobConstants.JobLeaseExpirationInSeconds)));
                return Task.FromResult(leaseId);
            }
        }

        public Task MoveDirectoryAsync(string sourceDirectory, string targetDirectory, CancellationToken cancellationToken = default)
        {
            foreach (var path in _blobStore.Keys)
            {
                if (path.StartsWith(sourceDirectory))
                {
                    var childPath = path.Substring(sourceDirectory.Length);
                    var newPath = $"{targetDirectory}{childPath}";
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
            foreach (var path in _blobStore.Keys)
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
            var directorySet = new HashSet<string>();
            var result = new List<PathItem>();
            foreach (var path in _blobStore.Keys)
            {
                if (path.StartsWith(directory) && !string.Equals(directory, path, StringComparison.OrdinalIgnoreCase))
                {
                    var pathItem = DataLakeModelFactory.PathItem(path, false, DateTimeOffset.UtcNow, new ETag("test"), 100, string.Empty, string.Empty, string.Empty);
                    yield return await Task.FromResult(pathItem);

                    var pathComponents = path.Split('/');
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
                            var directoryItem = DataLakeModelFactory.PathItem(baseDir, true, DateTimeOffset.UtcNow, new ETag("test"), 100, string.Empty, string.Empty, string.Empty);
                            yield return await Task.FromResult(directoryItem);
                        }
                    }
                }
            }
        }
    }
}
