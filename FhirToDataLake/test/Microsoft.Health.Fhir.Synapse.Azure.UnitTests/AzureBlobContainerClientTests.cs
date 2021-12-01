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
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Azure.Blob;
using Microsoft.Health.Fhir.Synapse.Azure.Exceptions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Azure.UnitTests
{
    [Trait("Category", "BlobTests")]
    public class AzureBlobContainerClientTests
    {
        private const string ConnectionString = "UseDevelopmentStorage=true";

        [Fact]
        public void CreateBlobProvider_ContainerShouldBeCreated()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            // The container doesn't exist at the beginning
            var blobServiceClient = new BlobServiceClient(ConnectionString);

            var container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.False(container.Exists());

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);

            // once blob provider instance is created, the container should exists
            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.True(container.Exists());

            // delete the created container
            blobServiceClient.DeleteBlobContainer(uniqueContainerName);
            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
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
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            // The container doesn't exist at the beginning
            var blobServiceClient = new BlobServiceClient(ConnectionString);

            var container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.False(container.Exists());

            AzureBlobContainerClient blobProvider_1 = GetTestBlobProvider(ConnectionString, uniqueContainerName);

            // once blob provider instance is created, the container should exists
            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.True(container.Exists());

            AzureBlobContainerClient blobProvider_2 = GetTestBlobProvider(ConnectionString, uniqueContainerName);

            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.True(container.Exists());

            // delete the created container
            blobServiceClient.DeleteBlobContainer(uniqueContainerName);
            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.False(container.Exists());
        }

        [Fact]
        public void GivenInvalidConnectionString_WhenCreateBlobProvider_ExceptionShouldBeThrown()
        {
            var errorFormatConncetionString = "invalidstring";
            Assert.Throws<FormatException>(() => GetTestBlobProvider(errorFormatConncetionString, errorFormatConncetionString));

            var emptyConncetionString = string.Empty;
            Assert.Throws<ArgumentNullException>(() => GetTestBlobProvider(emptyConncetionString, emptyConncetionString));

            var invalidAccountConncetionString = "DefaultEndpointsProtocol=https;AccountName=fakeaccountname;AccountKey=nbmHd6Y4U3qVgko7VFeFJEfHjX6dNFHUrqT+Kr0fXjwrEIDIv189v+iOljnQ1IYYK95Q2DoPK9KoDyy/T3yt3Q==;EndpointSuffix=core.windows.net";
            Assert.Throws<AzureBlobOperationFailedException>(() => GetTestBlobProvider(invalidAccountConncetionString, invalidAccountConncetionString));
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
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            // call function of blobProvider
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("example")))
            {
                Assert.True(await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None));
            }

            var blobServiceClient = new BlobServiceClient(ConnectionString);

            // delete the created container
            blobServiceClient.DeleteBlobContainer(uniqueContainerName);

            // call CreateBlobAsync() after the container is deleted by others
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None));
            }

            // call UploadStreamToBlobAsync()after the container is deleted by others
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => blobProvider.UpdateBlobAsync(blobName, stream, CancellationToken.None));
            }

            // call GetBlobAsync() after the container is deleted by others
            await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => blobProvider.GetBlobAsync(blobName, CancellationToken.None));
        }

        [Fact]
        public async void DeleteExistingBlob_TrueShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
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

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void DeleteNoExistingBlob_FalseShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            Assert.False(blobClient.Exists());

            // delete the blob
            var isDeleted = await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None);

            Assert.False(isDeleted);
            Assert.False(blobClient.Exists());

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void CreateNewBlobTwice_FalseShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
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

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void CreateDeletedBlob_TrueShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
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

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void CreateLargeNewBlobTwice_TrueShouldReturnForTheFirstCompleted_And_FalseShouldReturnForTheSecondOne()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
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

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void CreateLargeNewBlobTwice_AtTheSameTime_TrueShouldReturnForTheFirstCompleted_And_FalseShouldReturnForTheSecondOne()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
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
            var task_2 = blobProvider.CreateBlobAsync(blobName, sourceStream, CancellationToken.None);

            var isCreateds = await Task.WhenAll(task_1, task_2);

            Assert.True(blobClient.Exists());
            Assert.True(isCreateds[0] ^ isCreateds[1]);

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void DownloadBlob_StreamShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
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

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void DownloadNoExistingBlob_StreamShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            Assert.False(blobClient.Exists());

            using Stream downloadStream = await blobProvider.GetBlobAsync(blobName, CancellationToken.None);

            Assert.Null(downloadStream);

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void UpdateStreamToBlob_TheBlobShouldBeOverwriten()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            blobClient.DeleteIfExists();
            Assert.False(blobClient.Exists());

            // create a new blob
            string blobContent = "example";
            var blobUrl_1 = await blobProvider.UpdateBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(blobContent)), CancellationToken.None); ;

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
            var blobUrl_2 = await blobProvider.UpdateBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(newBlobContent)), CancellationToken.None);

            Assert.Equal(blobUrl_1, blobUrl_2);
            Assert.True(blobClient.Exists());

            using (var stream = new MemoryStream())
            {
                var res = await blobClient.DownloadToAsync(stream);
                stream.Position = 0;
                using var reader = new StreamReader(stream);
                Assert.Equal(newBlobContent, reader.ReadToEnd());
            }

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void AcquireLease_WhenALeaseExists_ExceptionShouldBeThrown()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
            var blobClient = blobContainerClient.GetBlobClient(blobName);
            blobClient.DeleteIfExists();
            Assert.False(blobClient.Exists());

            // create a new blob
            string blobContent = "example";
            var blobUrl_1 = await blobProvider.CreateBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(blobContent)), CancellationToken.None);
            Assert.True(blobClient.Exists());

            var lease = await blobProvider.AcquireLeaseAsync(blobName, null, TimeSpan.FromSeconds(30), default);
            Assert.NotNull(lease);

            var lease2 = await blobProvider.AcquireLeaseAsync(blobName, null, TimeSpan.FromSeconds(30), default);
            Assert.Null(lease2);

            var lease3 = await blobProvider.AcquireLeaseAsync(blobName, lease, TimeSpan.FromSeconds(30), default);
            Assert.Equal(lease, lease3);

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void AcquireLease_WhenBlobNotExists_NoLeaseShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");
            var lease = await blobProvider.AcquireLeaseAsync(blobName, null, TimeSpan.FromSeconds(30), default);

            Assert.Null(lease);

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
            await blobContainerClient.DeleteIfExistsAsync();
        }

        [Fact]
        public async void ReleaseLease_WhenBlobNotExists_NoLeaseShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");
            var result = await blobProvider.ReleaseLeaseAsync(blobName, null, default);

            Assert.False(result);

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
            await blobContainerClient.DeleteIfExistsAsync();
        }

        [Fact]
        public async void RenewLease_WhenBlobNotExists_NoLeaseShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");

            await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => blobProvider.RenewLeaseAsync(blobName, null, default));

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
            await blobContainerClient.DeleteIfExistsAsync();
        }

        private AzureBlobContainerClient GetTestBlobProvider(string connectionString, string containerName)
        {
            return new AzureBlobContainerClient(connectionString, containerName, new NullLogger<AzureBlobContainerClient>());
        }
    }
}
