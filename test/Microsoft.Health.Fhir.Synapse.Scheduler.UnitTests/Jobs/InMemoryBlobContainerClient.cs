// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Azure;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Scheduler.Jobs;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.UnitTests.Jobs
{
    public class InMemoryBlobContainerClient : IAzureBlobContainerClient
    {
        private ConcurrentDictionary<string, Stream> _blobStore = new ConcurrentDictionary<string, Stream>();
        private ConcurrentDictionary<string, string> _blobLeaseStore = new ConcurrentDictionary<string, string>();
        private object _leaseLock = new object();

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

        public async Task CreateJob(Job job, string jobName, string leaseId = null)
        {
            var jobConfig = new JobConfiguration
            {
                LastScheduledTimestamp = job.DataPeriod.End,
                EndTime = job.DataPeriod.End,
            };
            var jobConfigContent = JsonConvert.SerializeObject(jobConfig);
            var configStream = new MemoryStream(Encoding.UTF8.GetBytes(jobConfigContent));
            await CreateBlobAsync(JobConstants.JobConfigName, configStream);

            var jobContent = JsonConvert.SerializeObject(job);
            var stream = new MemoryStream(Encoding.UTF8.GetBytes(jobContent));
            if (!string.IsNullOrEmpty(leaseId))
            {
                _blobLeaseStore[jobName] = leaseId;
            }

            await CreateBlobAsync(jobName, stream);
        }

        public Task<bool> CreateBlobAsync(
            string blobName,
            Stream stream,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_blobStore.TryAdd(blobName, stream));
        }

        public Task<string> CreateOrUpdateStreamToBlobAsync(
            string blobName,
            Stream stream,
            CancellationToken cancellationToken = default)
        {
            _blobStore.AddOrUpdate(blobName, (_) => stream, (_, _) => stream);
            return Task.FromResult(blobName);
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

        public Task<bool> DeleteBlobAsync(string blobName, string leaseId = null, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(leaseId)
                || string.Equals(leaseId, _blobLeaseStore[blobName], StringComparison.OrdinalIgnoreCase))
            {
                return Task.FromResult(_blobStore.TryRemove(blobName, out _));
            }

            return Task.FromResult(false);
        }

        public Task<string> UploadBlobAsync(string blobName, Stream stream, bool overwrite = false, string leaseId = null, CancellationToken cancellationToken = default)
        {
            if (overwrite)
            {
                if (string.IsNullOrEmpty(leaseId)
                    || string.Equals(leaseId, _blobLeaseStore[blobName], StringComparison.OrdinalIgnoreCase))
                {
                    _blobStore.AddOrUpdate(blobName, (_) => stream, (_, _) => stream);
                }
            }
            else
            {
                _blobStore.TryAdd(blobName, stream);
            }

            return Task.FromResult(blobName);
        }

        public Task<string> AcquireLeaseAsync(string blobName, string leaseId, TimeSpan timeSpan, bool force = false, CancellationToken cancellationToken = default)
        {
            lock (_leaseLock)
            {
                if (force || !_blobLeaseStore.ContainsKey(blobName))
                {
                    string newLeaseId = Guid.NewGuid().ToString("N");
                    _blobLeaseStore[blobName] = newLeaseId;
                    return Task.FromResult(newLeaseId);
                }

                if (string.Equals(_blobLeaseStore[blobName], leaseId))
                {
                    return Task.FromResult(_blobLeaseStore[blobName]);
                }

                return Task.FromResult<string>(null);
            }
        }

        public Task<bool> ReleaseLeaseAsync(string blobName, string leaseId, CancellationToken cancellationToken = default)
        {
            lock (_leaseLock)
            {
                if (_blobLeaseStore.ContainsKey(blobName) && string.Equals(_blobLeaseStore[blobName], leaseId))
                {
                    return Task.FromResult(_blobLeaseStore.TryRemove(blobName, out _));
                }

                return Task.FromResult(false);
            }
        }
    }
}
