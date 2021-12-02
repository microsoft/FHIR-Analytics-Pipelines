// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Core;
using Microsoft.Health.Fhir.Transformation.Core;

namespace Microsoft.Health.Fhir.Transformation.Cdm.BatchExecutor
{
    public class StorageBlobNdjsonSource : ISource, IDisposable
    {
        public FhirBlobDataStream _stream;
        private StreamReader _reader;

        private Uri _uri;
        private TokenCredential _credential;

        public StorageBlobNdjsonSource(Uri blobUri, TokenCredential credential)
        {
            _uri = blobUri;
            _credential = credential;
        }

        public int ConcurrentCount
        {
            get;
            set;
        } = FhirAzureConstants.DefaultConcurrentCount;

        public int BlockDownloadTimeoutInSeconds
        {
            get;
            set;
        } = FhirAzureConstants.DefaultBlockDownloadTimeoutInSeconds;

        public int BlockDownloadTimeoutRetryCount
        {
            get;
            set;
        } = FhirAzureConstants.DefaultBlockDownloadTimeoutRetryCount;

        public Task CloseAsync()
        {
            Dispose(false);
            return Task.CompletedTask;
        }

        public Task OpenAsync()
        {
            _stream = new FhirBlobDataStream(_uri, _credential)
            {
                ConcurrentCount = ConcurrentCount,
                BlockDownloadTimeoutInSeconds = BlockDownloadTimeoutInSeconds,
                BlockDownloadTimeoutRetryCount = BlockDownloadTimeoutRetryCount
            };
            _reader = new StreamReader(_stream);

            return Task.CompletedTask;
        }

        public async Task<string> ReadAsync()
        {
            return await _reader.ReadLineAsync();
        }

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _reader?.Dispose();
                    _stream?.Dispose();
                }

                disposedValue = true;
                _stream = null;
                _reader = null;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
