// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Azure.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Azure.UnitTests
{
    [Trait("Category", "BlobTests")]
    public class AzureBlobContainerClientTests
    {
        private const string ConnectionString = "UseDevelopmentStorage=true";
        private const string TestContainerName = "synapseunittests";

        [Fact]
        public void CreateBlobProvider_ContainerShouldBeCreated()
        {
            // The container doesn't exist at the beginning
            var blobServiceClient = new BlobServiceClient(ConnectionString);

            var container = blobServiceClient.GetBlobContainerClient(TestContainerName);
            Assert.False(container.Exists());

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);

            // once blob provider instance is created, the container should exists
            container = blobServiceClient.GetBlobContainerClient(TestContainerName);
            Assert.True(container.Exists());

            // delete the created container
            blobServiceClient.DeleteBlobContainer(TestContainerName);
            container = blobServiceClient.GetBlobContainerClient(TestContainerName);
            Assert.False(container.Exists());
        }

        [Fact]
        public void CreateBlobProvider_WhenContainerExists_NoExceptionshouldBeThrown()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            // The container doesn't exist at the beginning
            var blobServiceClient = new BlobServiceClient(ConnectionString);

            var container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.False(container.Exists());

            // create the container
            container = blobServiceClient.CreateBlobContainer(uniqueContainerName);
            Assert.True(container.Exists());

            // no exception should be thrown if the container exists
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);

            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.True(container.Exists());

            // delete the created container
            blobServiceClient.DeleteBlobContainer(uniqueContainerName);
            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.False(container.Exists());
        }

        [Fact]
        public void CreateTwoBlobProvides_NoExceptionshouldBeThrown()
        {
            // The container doesn't exist at the beginning
            var blobServiceClient = new BlobServiceClient(ConnectionString);

            var container = blobServiceClient.GetBlobContainerClient(TestContainerName);
            Assert.False(container.Exists());

            AzureBlobContainerClient blobProvider_1 = GetTestBlobProvider(ConnectionString, TestContainerName);

            // once blob provider instance is created, the container should exists
            container = blobServiceClient.GetBlobContainerClient(TestContainerName);
            Assert.True(container.Exists());

            AzureBlobContainerClient blobProvider_2 = GetTestBlobProvider(ConnectionString, TestContainerName);

            container = blobServiceClient.GetBlobContainerClient(TestContainerName);
            Assert.True(container.Exists());

            // delete the created container
            blobServiceClient.DeleteBlobContainer(TestContainerName);
            container = blobServiceClient.GetBlobContainerClient(TestContainerName);
            Assert.False(container.Exists());
        }

        [Fact]
        public void GivenInvalidConnectionString_WhenCreateBlobProvider_ExceptionShouldBeThrown()
        {
            var errorFormatConncetionString = "invalidstring";
            Assert.Throws<FormatException>(() => GetTestBlobProvider(errorFormatConncetionString, TestContainerName));

            var emptyConncetionString = string.Empty;
            Assert.Throws<ArgumentNullException>(() => GetTestBlobProvider(emptyConncetionString, TestContainerName));

            var invalidAccountConncetionString = "DefaultEndpointsProtocol=https;AccountName=fakeaccountname;AccountKey=nbmHd6Y4U3qVgko7VFeFJEfHjX6dNFHUrqT+Kr0fXjwrEIDIv189v+iOljnQ1IYYK95Q2DoPK9KoDyy/T3yt3Q==;EndpointSuffix=core.windows.net";
            Assert.Throws<AzureBlobOperationFailedException>(() => GetTestBlobProvider(invalidAccountConncetionString, TestContainerName));
        }

        [Fact]
        public void GivenEmptyContainerName_WhenCreateBlobProvider_ExceptionShouldBeThrown()
        {
            var emptyConatinerName = string.Empty;
            Assert.Throws<AzureBlobOperationFailedException>(() => GetTestBlobProvider(ConnectionString, emptyConatinerName));
        }

        [Fact]
        public async void AccessBlobProvider_WhenContainerNoExists_ExceptionShouldBeThrown()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            // call function of blobProvider
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("example")))
            {
                Assert.True(await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None));
            }

            var blobServiceClient = new BlobServiceClient(ConnectionString);

            // delete the created container
            blobServiceClient.DeleteBlobContainer(TestContainerName);

            // call CreateBlobAsync() after the container is deleted by others
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                Assert.True(await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None));
            }

            blobServiceClient.DeleteBlobContainer(TestContainerName);

            // call UploadStreamToBlobAsync()after the container is deleted by others
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                Assert.NotNull(await blobProvider.UploadBlobAsync(blobName, stream, false, cancellationToken: CancellationToken.None));
            }

            blobServiceClient.DeleteBlobContainer(TestContainerName);

            // call CreateOrUpdateStreamToBlobAsync() after the container is deleted by others
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                Assert.NotNull(await blobProvider.UploadBlobAsync(blobName, stream, true, cancellationToken: CancellationToken.None));
            }

            blobServiceClient.DeleteBlobContainer(TestContainerName);

            // call GetBlobAsync() after the container is deleted by others
            Assert.Null(await blobProvider.GetBlobAsync(blobName, CancellationToken.None));

            blobServiceClient.DeleteBlobContainer(TestContainerName);

            // call GetBlobAsync() after the container is deleted by others
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                Assert.True(await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None));
                Assert.NotNull(await blobProvider.GetBlobAsync(blobName, CancellationToken.None));
            }

            blobServiceClient.DeleteBlobContainer(TestContainerName);

            // call DeleteBlobAsync() after the container is deleted by others
            Assert.False(await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None));

            // delete the created container
            blobServiceClient.DeleteBlobContainer(TestContainerName);
        }

        [Fact]
        public async void DeleteExistingBlob_TrueShouldReturn()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            Assert.False(blobClient.Exists());

            // create a new blob
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("example")))
            {
                var result = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);

                Assert.True(result);
                Assert.True(blobClient.Exists());
            }

            // delete the blob
            var isDeleted = await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None);

            Assert.True(isDeleted);
            Assert.False(blobClient.Exists());

            // delete the blob again
            isDeleted = await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None);

            Assert.False(isDeleted);
            Assert.False(blobClient.Exists());

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void DeleteNoExistingBlob_FalseShouldReturn()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            Assert.False(blobClient.Exists());

            // delete the blob
            var isDeleted = await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None);

            Assert.False(isDeleted);
            Assert.False(blobClient.Exists());

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void CreateNewBlobTwice_FalseShouldReturn()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            await blobClient.DeleteIfExistsAsync();
            Assert.False(blobClient.Exists());

            // create a new blob
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("example")))
            {
                var isCreated = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);
                Assert.True(isCreated);
                Assert.True(blobClient.Exists());
            }

            // create the blob again
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                var isCreated = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);
                Assert.False(isCreated);
                Assert.True(blobClient.Exists());
            }

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void CreateDeletedBlob_TrueShouldReturn()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            await blobClient.DeleteIfExistsAsync();
            Assert.False(blobClient.Exists());

            // create a new blob
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("example")))
            {
                var isCreated = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);
                Assert.True(isCreated);
                Assert.True(blobClient.Exists());
            }

            // delete the blob
            var isDeleted = await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None);

            Assert.True(isDeleted);
            Assert.False(blobClient.Exists());

            // create the blob again
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                var isCreated = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);
                Assert.True(isCreated);
                Assert.True(blobClient.Exists());
            }

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void CreateLargeNewBlobTwice_TrueShouldReturnForTheFirstCompleted_And_FalseShouldReturnForTheSecondOne()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            await blobClient.DeleteIfExistsAsync();

            using MemoryStream sourceStream = new MemoryStream();
            using StreamWriter writer = new StreamWriter(sourceStream);

            int lineNumber = (1024 * 1024) + 3;
            while (lineNumber-- > 0)
            {
                await writer.WriteLineAsync(Guid.NewGuid().ToString("N"));
            }

            await writer.FlushAsync();

            sourceStream.Position = 0;

            Assert.False(blobClient.Exists());

            var task_1 = blobProvider.CreateBlobAsync(blobName, sourceStream, CancellationToken.None);
            var task_2 = blobProvider.CreateBlobAsync(blobName, sourceStream, CancellationToken.None);

            var isCreated_1 = await task_1;
            var isCreated_2 = await task_2;
            Assert.True(isCreated_1 ^ isCreated_2);

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void CreateLargeNewBlobTwice_AtTheSameTime_TrueShouldReturnForTheFirstCompleted_And_FalseShouldReturnForTheSecondOne()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            await blobClient.DeleteIfExistsAsync();
            Assert.False(blobClient.Exists());

            using MemoryStream sourceStream = new MemoryStream();
            using StreamWriter writer = new StreamWriter(sourceStream);

            int lineNumber = (1024 * 1024) + 3;
            while (lineNumber-- > 0)
            {
                await writer.WriteLineAsync(Guid.NewGuid().ToString("N"));
            }

            await writer.FlushAsync();

            sourceStream.Position = 0;
            var task_1 = blobProvider.CreateBlobAsync(blobName, sourceStream, CancellationToken.None);
            Assert.False(blobClient.Exists());

            sourceStream.Position = 0;
            var task_2 = blobProvider.CreateBlobAsync(blobName, sourceStream, CancellationToken.None);
            Assert.False(blobClient.Exists());

            var isCreateds = await Task.WhenAll(task_1, task_2);

            Assert.True(isCreateds[0] ^ isCreateds[1]);

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void DownloadBlob_StreamShouldReturn()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);

            string blobContent = "example";

            // create a new blob
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(blobContent)))
            {
                var isCreated = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);
                Assert.True(isCreated);
                Assert.True(blobClient.Exists());
            }

            using Stream downloadStream = await blobProvider.GetBlobAsync(blobName, CancellationToken.None);

            using var reader = new StreamReader(downloadStream);
            Assert.Equal(blobContent, reader.ReadToEnd());

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void DownloadNoExistingBlob_StreamShouldReturn()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            Assert.False(blobClient.Exists());

            using Stream downloadStream = await blobProvider.GetBlobAsync(blobName, CancellationToken.None);

            Assert.Null(downloadStream);

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void CreateOrUpdateStreamToBlob_TheBlobShouldBeOverwriten()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            blobClient.DeleteIfExists();
            Assert.False(blobClient.Exists());

            // create a new blob
            string blobContent = "example";
            var blobUrl_1 = await blobProvider.UploadBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(blobContent)), true, cancellationToken: CancellationToken.None); ;

            Assert.True(blobClient.Exists());

            using (var stream = new MemoryStream())
            {
                var res = await blobClient.DownloadToAsync(stream);
                stream.Position = 0;
                using var reader = new StreamReader(stream);
                Assert.Equal(blobContent, reader.ReadToEnd());
            }

            // upload to a existing blob
            string newBlobContent = "new example";
            var blobUrl_2 = await blobProvider.UploadBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(newBlobContent)), true, cancellationToken: CancellationToken.None);

            Assert.Equal(blobUrl_1, blobUrl_2);
            Assert.True(blobClient.Exists());

            using (var stream = new MemoryStream())
            {
                var res = await blobClient.DownloadToAsync(stream);
                stream.Position = 0;
                using var reader = new StreamReader(stream);
                Assert.Equal(newBlobContent, reader.ReadToEnd());
            }

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void UploadStreamToBlob_DisableOverwrite_WhenBlobExists_ExceptionShouldBeThrown()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            blobClient.DeleteIfExists();
            Assert.False(blobClient.Exists());

            // create a new blob
            string blobContent = "example";
            var blobUrl_1 = await blobProvider.UploadBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(blobContent)), false, cancellationToken: CancellationToken.None);

            // upload to a existing blob
            string newBlobContent = "new example";
            await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => blobProvider.UploadBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(newBlobContent)), false, cancellationToken: CancellationToken.None));

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async Task UploadStreamWithLease_TheBlobShouldBeOverwriten()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            blobClient.DeleteIfExists();
            Assert.False(blobClient.Exists());

            // create a new blob
            string blobContent = "example";
            string leaseId = await blobProvider.AcquireLeaseAsync(blobName, null, TimeSpan.FromSeconds(-1));
            var result = await blobProvider.UploadBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(blobContent)), true, leaseId, CancellationToken.None);
            Assert.NotEmpty(result);
            Assert.True(blobClient.Exists());

            // overwrite the blob content
            string newBlobContent = "new example";
            result = await blobProvider.UploadBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(newBlobContent)), true, leaseId, CancellationToken.None);
            Assert.NotEmpty(result);
            Assert.True(blobClient.Exists());

            using (var stream = new MemoryStream())
            {
                var res = await blobClient.DownloadToAsync(stream);
                stream.Position = 0;
                using var reader = new StreamReader(stream);
                Assert.Equal(newBlobContent, reader.ReadToEnd());
            }

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void UploadStreamWithLease_WhenLeaseUsedByOthers_FalseShouldReturn()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            blobClient.DeleteIfExists();
            Assert.False(blobClient.Exists());

            // the blob doesn't exist, so fail to acquire lease
            string leaseId_1 = Guid.NewGuid().ToString("N");
            var leaseId = await blobProvider.AcquireLeaseAsync(blobName, leaseId_1, BlobLeaseClient.InfiniteLeaseDuration, false, CancellationToken.None);
            Assert.Null(leaseId);

            // create a new blob
            string blobContent = "example";
            string leaseId_2 = await blobProvider.AcquireLeaseAsync(blobName, leaseId_1, BlobLeaseClient.InfiniteLeaseDuration, false, CancellationToken.None);
            var result = await blobProvider.UploadBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(blobContent)), true, leaseId_2, CancellationToken.None);
            Assert.NotNull(result);
            Assert.True(blobClient.Exists());

            // the lease is acquired
            leaseId = await blobProvider.AcquireLeaseAsync(blobName, leaseId_1, BlobLeaseClient.InfiniteLeaseDuration, false, CancellationToken.None);
            Assert.NotNull(leaseId);

            // the lease is acauired by others, upload will fail
            string newBlobContent = "new example";
            await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => blobProvider.UploadBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(newBlobContent)), true, leaseId_2, CancellationToken.None));
            Assert.True(blobClient.Exists());

            // the lease id is leaseId_1, try to release leaseId_2 will fail
            var isLeaseReleased = await blobProvider.ReleaseLeaseAsync(blobName, leaseId_2, CancellationToken.None);
            Assert.False(isLeaseReleased);

            // release lease
            isLeaseReleased = await blobProvider.ReleaseLeaseAsync(blobName, leaseId_1, CancellationToken.None);
            Assert.True(isLeaseReleased);

            // the lease is released, upload will success
            result = await blobProvider.UploadBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(newBlobContent)), true, leaseId_2, CancellationToken.None);
            Assert.NotNull(result);

            using (var stream = new MemoryStream())
            {
                var res = await blobClient.DownloadToAsync(stream);
                stream.Position = 0;
                using var reader = new StreamReader(stream);
                Assert.Equal(newBlobContent, reader.ReadToEnd());
            }

            blobContainerClient.DeleteIfExists();
        }

        [Fact]
        public async void UploadLargeStreamWithLeaseTwice_AtTheSameTime_OneWillFail()
        {
            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, TestContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, TestContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            blobClient.DeleteIfExists();
            Assert.False(blobClient.Exists());

            // create two tasks to upload stream at the same time with different lease id
            using MemoryStream sourceStream = new MemoryStream();
            using StreamWriter writer = new StreamWriter(sourceStream);

            int lineNumber = (1024 * 1024) + 3;
            while (lineNumber-- > 0)
            {
                await writer.WriteLineAsync(Guid.NewGuid().ToString("N"));
            }

            await writer.FlushAsync();

            // Create blob.
            await blobProvider.UploadBlobAsync(blobName, sourceStream);

            string leaseId_1 = await blobProvider.AcquireLeaseAsync(blobName, null, TimeSpan.FromSeconds(-1));
            var task_1 = blobProvider.UploadBlobAsync(blobName, sourceStream, true, leaseId_1, CancellationToken.None);

            string leaseId_2 = await blobProvider.AcquireLeaseAsync(blobName, null, TimeSpan.FromSeconds(-1), true);
            var task_2 = blobProvider.UploadBlobAsync(blobName, sourceStream, true, leaseId_2, CancellationToken.None);

            var results = await Task.WhenAll(task_1, task_2);

            Assert.NotNull(results[0]);
            Assert.NotNull(results[1]);

            // create two tasks to upload stream at the same time with different lease id, since blob exists, they will both upload with lease
            task_1 = blobProvider.UploadBlobAsync(blobName, sourceStream, true, leaseId_1, CancellationToken.None);
            task_2 = blobProvider.UploadBlobAsync(blobName, sourceStream, true, leaseId_2, CancellationToken.None);

            await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => Task.WhenAll(task_1, task_2));

            // second task will success, first will fail due to lease expired.
            Assert.True(task_1.IsFaulted);
            Assert.False(task_2.IsFaulted);

            blobContainerClient.DeleteIfExists();
        }

        private AzureBlobContainerClient GetTestBlobProvider(string connectionString, string containerName)
        {
            return new AzureBlobContainerClient(connectionString, containerName, new NullLogger<AzureBlobContainerClient>());
        }
    }
}
