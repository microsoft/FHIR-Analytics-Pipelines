// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Health.Fhir.Transformation.BatchExecutor;
using Microsoft.Health.Fhir.Transformation.Cdm.BatchExecutor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Assert = Xunit.Assert;


namespace Microsoft.Health.Fhir.Transformation.Cdm.Test
{
    [TestClass]
    public class StorageBlobNdjsonSourceTest
    {
        private TestSettings _testSettings;

        public StorageBlobNdjsonSourceTest()
        {
            _testSettings = TestUtils.LoadTestSettings();
        }

        [TestMethod]
        public async Task GivenAStorageBlobNdjsonSource_WhenDownloadData_AllDataShouldbeReturned_Async()
        {
            if (string.IsNullOrEmpty(_testSettings.BlobUri))
            {
                return;
            }
            var credential = Program.GetClientSecretCredential(_testSettings.TenantId, _testSettings.ClientId, _testSettings.Secret);

            string containerName = Guid.NewGuid().ToString("N");
            string blobName = Guid.NewGuid().ToString("N");

            Uri containerUri = new Uri(_testSettings.BlobUri + "/" + containerName);
            Uri docUri = new Uri(_testSettings.BlobUri + "/" + containerName + "/" + blobName);
            BlobContainerClient containerClient = new BlobContainerClient(containerUri, credential);
            var source = new StorageBlobNdjsonSource(docUri, credential);
            try
            {
                await source.OpenAsync();
                await containerClient.CreateIfNotExistsAsync();
                var blobClient = containerClient.GetBlobClient(blobName);

                List<string> expectedResult = GenerateTestBlobAsync(blobClient).Result;

                for (int i = 0; i < expectedResult.Count; ++i)
                {
                    var content = await source.ReadAsync();
                    Assert.Equal(expectedResult[i], content);
                }
            }
            finally
            {
                await source.CloseAsync();
                await containerClient.DeleteIfExistsAsync();
            }
            return;
        }

        [TestMethod]
        public async Task GivenAStorageBlobNdjsonSource_WhenDownloadDataTimeout_OperationShouldBeRetried_Async()
        {
            if (string.IsNullOrEmpty(_testSettings.BlobUri))
            {
                return;
            }
            var credential = Program.GetClientSecretCredential(_testSettings.TenantId, _testSettings.ClientId, _testSettings.Secret);
            string containerName = Guid.NewGuid().ToString("N");
            string blobName = Guid.NewGuid().ToString("N");

            Uri containerUri = new Uri(_testSettings.BlobUri + "/" + containerName);
            Uri docUri = new Uri(_testSettings.BlobUri + "/" + containerName + "/" + blobName);
            BlobContainerClient containerClient = new BlobContainerClient(containerUri, credential);
            var source = new StorageBlobNdjsonSource(docUri, credential);

            try
            {
                await containerClient.CreateIfNotExistsAsync();
                var blobClient = containerClient.GetBlobClient(blobName);

                List<string> expectedResult = GenerateTestBlobAsync(blobClient).Result;

                Dictionary<long, int> enterRecord = new Dictionary<long, int>();
                source.BlockDownloadTimeoutRetryCount = 1;
                source.BlockDownloadTimeoutInSeconds = 5;
                await source.OpenAsync();

                source._stream.DownloadDataFunc = async (client, range) =>
                {
                    if (!enterRecord.ContainsKey(range.Offset))
                    {
                        enterRecord[range.Offset] = 0;
                    }

                    if (enterRecord[range.Offset]++ < 1)
                    {
                        Thread.Sleep(TimeSpan.FromSeconds(10));
                    }

                    var downloadInfo = await client.DownloadAsync(range);
                    return downloadInfo.Value.Content;
                };

                for (int i = 0; i < expectedResult.Count; ++i)
                {
                    var content = await source.ReadAsync();
                    Assert.Equal(expectedResult[i], content);
                }

                foreach (int count in enterRecord.Values)
                {
                    Assert.Equal(2, count);
                }
            }
            finally
            {
                await source.CloseAsync();
                await containerClient.DeleteIfExistsAsync();
            }
            return;
        }

        private static async Task<List<string>> GenerateTestBlobAsync(BlobClient blobClient)
        {
            Random random = new Random();
            MemoryStream stream = new MemoryStream();
            StreamWriter writer = new StreamWriter(stream, encoding: Encoding.UTF8);
            int lines = 0;
            var expectedResult = new List<string>();
            while (lines++ < 128)
            {
                string content = new string('*', random.Next(2, 1024 * 128)) + "aA!1·\t中";
                await writer.WriteLineAsync(content);
                expectedResult.Add(content);
            }

            writer.Flush();

            stream.Position = 0;
            await blobClient.UploadAsync(stream);
            return expectedResult;
        }
    }
}
