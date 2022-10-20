// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.UnitTests.Azure
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

            _ = GetTestBlobProvider(ConnectionString, uniqueContainerName);

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
            _ = GetTestBlobProvider(ConnectionString, uniqueContainerName);

            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.True(container.Exists());

            // delete the created container
            blobServiceClient.DeleteBlobContainer(uniqueContainerName);
            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.False(container.Exists());
        }

        [Fact]
        public void CreateTwoBlobProviders_NoExceptionshouldBeThrown()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            // The container doesn't exist at the beginning
            var blobServiceClient = new BlobServiceClient(ConnectionString);

            var container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.False(container.Exists());

            _ = GetTestBlobProvider(ConnectionString, uniqueContainerName);

            // once blob provider instance is created, the container should exists
            container = blobServiceClient.GetBlobContainerClient(uniqueContainerName);
            Assert.True(container.Exists());

            _ = GetTestBlobProvider(ConnectionString, uniqueContainerName);

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
            await blobServiceClient.DeleteBlobContainerAsync(uniqueContainerName);

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
            Assert.False(await blobClient.ExistsAsync());

            // create a new blob
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("example")))
            {
                var result = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);

                Assert.True(result);
                Assert.True(await blobClient.ExistsAsync());
            }

            // delete the blob
            var isDeleted = await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None);

            Assert.True(isDeleted);
            Assert.False(await blobClient.ExistsAsync());

            // delete the blob again
            isDeleted = await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None);

            Assert.False(isDeleted);
            Assert.False(await blobClient.ExistsAsync());

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
            Assert.False(await blobClient.ExistsAsync());

            // delete the blob
            var isDeleted = await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None);

            Assert.False(isDeleted);
            Assert.False(await blobClient.ExistsAsync());

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
            Assert.False(await blobClient.ExistsAsync());

            // create a new blob
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("example")))
            {
                var isCreated = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);
                Assert.True(isCreated);
                Assert.True(await blobClient.ExistsAsync());
            }

            // create the blob again
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                var isCreated = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);
                Assert.False(isCreated);
                Assert.True(await blobClient.ExistsAsync());
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
            Assert.False(await blobClient.ExistsAsync());

            // create a new blob
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("example")))
            {
                var isCreated = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);
                Assert.True(isCreated);
                Assert.True(await blobClient.ExistsAsync());
            }

            // delete the blob
            var isDeleted = await blobProvider.DeleteBlobAsync(blobName, cancellationToken: CancellationToken.None);

            Assert.True(isDeleted);
            Assert.False(await blobClient.ExistsAsync());

            // create the blob again
            using (MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes("new example")))
            {
                var isCreated = await blobProvider.CreateBlobAsync(blobName, stream, CancellationToken.None);
                Assert.True(isCreated);
                Assert.True(await blobClient.ExistsAsync());
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

            Assert.False(await blobClient.ExistsAsync());

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

            Assert.False(await blobClient.ExistsAsync());

            using MemoryStream sourceStream_1 = new MemoryStream();
            using StreamWriter writer = new StreamWriter(sourceStream_1);

            int lineNumber = (1024 * 1024) + 3;
            while (lineNumber-- > 0)
            {
                await writer.WriteLineAsync(Guid.NewGuid().ToString("N"));
            }

            await writer.FlushAsync();

            sourceStream_1.Position = 0;

            using MemoryStream sourceStream_2 = new MemoryStream();
            await sourceStream_1.CopyToAsync(sourceStream_2);

            sourceStream_1.Position = 0;
            sourceStream_2.Position = 0;

            var task_1 = blobProvider.CreateBlobAsync(blobName, sourceStream_1, CancellationToken.None);
            var task_2 = blobProvider.CreateBlobAsync(blobName, sourceStream_2, CancellationToken.None);

            var isCreateds = await Task.WhenAll(task_1, task_2);

            Assert.True(await blobClient.ExistsAsync());
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
                Assert.True(await blobClient.ExistsAsync());
            }

            using Stream downloadStream = await blobProvider.GetBlobAsync(blobName, CancellationToken.None);

            using var reader = new StreamReader(downloadStream);
            Assert.Equal(blobContent, await reader.ReadToEndAsync());

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
            Assert.False(await blobClient.ExistsAsync());

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
            await blobClient.DeleteIfExistsAsync();
            Assert.False(await blobClient.ExistsAsync());

            // create a new blob
            string blobContent = "example";
            var blobUrl_1 = await blobProvider.UpdateBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(blobContent)), CancellationToken.None);

            Assert.True(await blobClient.ExistsAsync());

            using (var stream = new MemoryStream())
            {
                var res = await blobClient.DownloadToAsync(stream);
                stream.Position = 0;
                using var reader = new StreamReader(stream);
                Assert.Equal(blobContent, await reader.ReadToEndAsync());
            }

            // upload to a existing blob
            string newBlobContent = "new example";
            var blobUrl_2 = await blobProvider.UpdateBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(newBlobContent)), CancellationToken.None);

            Assert.Equal(blobUrl_1, blobUrl_2);
            Assert.True(await blobClient.ExistsAsync());

            using (var stream = new MemoryStream())
            {
                var res = await blobClient.DownloadToAsync(stream);
                stream.Position = 0;
                using var reader = new StreamReader(stream);
                Assert.Equal(newBlobContent, await reader.ReadToEndAsync());
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
            await blobClient.DeleteIfExistsAsync();
            Assert.False(await blobClient.ExistsAsync());

            // create a new blob
            string blobContent = "example";
            var blobUrl_1 = await blobProvider.CreateBlobAsync(blobName, new MemoryStream(Encoding.ASCII.GetBytes(blobContent)), CancellationToken.None);
            Assert.True(await blobClient.ExistsAsync());

            var lease = await blobProvider.AcquireLeaseAsync(blobName, null, TimeSpan.FromSeconds(30));
            Assert.NotNull(lease);

            var lease2 = await blobProvider.AcquireLeaseAsync(blobName, null, TimeSpan.FromSeconds(30));
            Assert.Null(lease2);

            var lease3 = await blobProvider.AcquireLeaseAsync(blobName, lease, TimeSpan.FromSeconds(30));
            Assert.Equal(lease, lease3);

            await blobContainerClient.DeleteAsync();
        }

        [Fact]
        public async void AcquireLease_WhenBlobNotExists_NoLeaseShouldReturn()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");

            AzureBlobContainerClient blobProvider = GetTestBlobProvider(ConnectionString, uniqueContainerName);
            string blobName = Guid.NewGuid().ToString("N");
            var lease = await blobProvider.AcquireLeaseAsync(blobName, null, TimeSpan.FromSeconds(30));

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
            var result = await blobProvider.ReleaseLeaseAsync(blobName, null);

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

            await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => blobProvider.RenewLeaseAsync(blobName, null));

            var blobContainerClient = new BlobContainerClient(ConnectionString, uniqueContainerName);
            await blobContainerClient.DeleteIfExistsAsync();
        }

        [SkippableFact]
        public async Task GivenANotExistingDirectoryName_WhenDeleteIfExists_NoExceptionShouldBeThrown()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            AzureBlobContainerClient adlsClient = GetTestAdlsGen2Client(uniqueContainerName);
            Skip.If(adlsClient == null);

            var blobContainerClient = GetBlobContainerClient(uniqueContainerName);
            await blobContainerClient.CreateIfNotExistsAsync();

            try
            {
                var exception = await Record.ExceptionAsync(async () => await adlsClient.DeleteDirectoryIfExistsAsync("foldernotexist"));
                Assert.Null(exception);
            }
            finally
            {
                await blobContainerClient.DeleteAsync();
            }
        }

        [SkippableFact]
        public async Task GivenAValidDirectoryName_WhenDeleteIfExists_DirectoryAndBlobsShouldBeDeleted()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            AzureBlobContainerClient adlsClient = GetTestAdlsGen2Client(uniqueContainerName);
            Skip.If(adlsClient == null);

            var blobContainerClient = GetBlobContainerClient(uniqueContainerName);
            await blobContainerClient.CreateIfNotExistsAsync();

            try
            {
                var blobName = "foo/bar1/1.txt";
                await blobContainerClient.UploadBlobAsync(blobName, new MemoryStream(new byte[] { 1, 2, 3 }));
                var blobClient = blobContainerClient.GetBlobClient(blobName);
                Assert.True(await blobClient.ExistsAsync());
                await adlsClient.DeleteDirectoryIfExistsAsync("foo/bar1");
                Assert.False(await blobClient.ExistsAsync());
            }
            finally
            {
                await blobContainerClient.DeleteAsync();
            }
        }

        [SkippableFact]
        public async Task GivenANotExistingDirectoryName_WhenListPaths_NoPathShouldBeReturned()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            AzureBlobContainerClient adlsClient = GetTestAdlsGen2Client(uniqueContainerName);
            Skip.If(adlsClient == null);

            var blobContainerClient = GetBlobContainerClient(uniqueContainerName);
            await blobContainerClient.CreateIfNotExistsAsync();

            try
            {
                var paths = new List<PathItem>();
                await foreach (var item in adlsClient.ListPathsAsync(uniqueContainerName))
                {
                    paths.Add(item);
                }

                Assert.Empty(paths);
            }
            finally
            {
                await blobContainerClient.DeleteAsync();
            }
        }

        [SkippableFact]
        public async Task GivenAValidDirectoryName_WhenListPaths_AllPathsShouldBeReturned()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            AzureBlobContainerClient adlsClient = GetTestAdlsGen2Client(uniqueContainerName);
            Skip.If(adlsClient == null);

            var blobContainerClient = GetBlobContainerClient(uniqueContainerName);
            await blobContainerClient.CreateIfNotExistsAsync();

            try
            {
                // Set up directory info
                var blobList = new List<string> { "foo/bar/1.txt", "foo/bar1/1.txt", "foo/bar2/2.txt", "foo/bar2/2.1.txt", "foo/bar3/3.txt" };
                var expectedResult = new HashSet<string> { "foo/bar", "foo/bar1", "foo/bar2", "foo/bar3", "foo/bar/1.txt", "foo/bar1/1.txt", "foo/bar2/2.txt", "foo/bar2/2.1.txt", "foo/bar3/3.txt" };
                foreach (var blob in blobList)
                {
                    await blobContainerClient.UploadBlobAsync(blob, new MemoryStream(new byte[] { 1, 2, 3 }));
                }

                var paths = new List<PathItem>();
                await foreach (var item in adlsClient.ListPathsAsync("foo"))
                {
                    paths.Add(item);
                }

                Assert.Equal(expectedResult, paths.Select(x => x.Name).ToHashSet());
            }
            finally
            {
                await blobContainerClient.DeleteAsync();
            }
        }

        [SkippableFact]
        public async Task GivenANotExistingDirectoryName_WhenMoveDirectory_ExceptionShouldBeThrown()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            AzureBlobContainerClient adlsClient = GetTestAdlsGen2Client(uniqueContainerName);
            Skip.If(adlsClient == null);

            var blobContainerClient = GetBlobContainerClient(uniqueContainerName);
            await blobContainerClient.CreateIfNotExistsAsync();

            try
            {
                await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => adlsClient.MoveDirectoryAsync("foldernotexist", "dest"));
            }
            finally
            {
                await blobContainerClient.DeleteAsync();
            }
        }

        [SkippableFact]
        public async Task GivenADirectoryName_WhenMoveDirectories_AllSubDirectoriesShouldBeMoved()
        {
            var uniqueContainerName = Guid.NewGuid().ToString("N");
            AzureBlobContainerClient adlsClient = GetTestAdlsGen2Client(uniqueContainerName);
            Skip.If(adlsClient == null);

            var blobContainerClient = GetBlobContainerClient(uniqueContainerName);
            await blobContainerClient.CreateIfNotExistsAsync();

            try
            {
                // Set up directory info
                var blobList = new List<string> { "foo/bar/1.txt", "foo/bar1/1.txt", "foo/bar2/2.txt", "foo/bar2/2.1.txt", "foo/bar3/3.txt" };
                var expectedBlobs = new List<string> { "boo/bar/1.txt", "boo/bar1/1.txt", "boo/bar2/2.txt", "boo/bar2/2.1.txt", "boo/bar3/3.txt" };
                foreach (var blob in blobList)
                {
                    await blobContainerClient.UploadBlobAsync(blob, new MemoryStream(new byte[] { 1, 2, 3 }));
                }

                await adlsClient.MoveDirectoryAsync("foo", "boo");
                foreach (var expectedBlob in expectedBlobs)
                {
                    var blob = blobContainerClient.GetBlobClient(expectedBlob);
                    Assert.True(await blob.ExistsAsync());
                }
            }
            finally
            {
                await blobContainerClient.DeleteAsync();
            }
        }

        private AzureBlobContainerClient GetTestBlobProvider(string connectionString, string containerName)
        {
            return new AzureBlobContainerClient(connectionString, containerName, new DiagnosticLogger(), new NullLogger<AzureBlobContainerClient>());
        }

        // Get Azure Data Lake store gen2 account connection string.
        // Some operations like GetSubDirectories and MoveDirectory is not supported in Azure Storage Emulator.
        private static AzureBlobContainerClient GetTestAdlsGen2Client(string containerName)
        {
            var storageUrl = GetAdlsGen2StoreUrl();
            if (string.IsNullOrEmpty(storageUrl))
            {
                return null;
            }

            return new AzureBlobContainerClient(new Uri(new Uri(storageUrl), containerName), new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()), new DiagnosticLogger(), new NullLogger<AzureBlobContainerClient>());
        }

        private static BlobContainerClient GetBlobContainerClient(string containerName)
        {
            var storageUrl = GetAdlsGen2StoreUrl();
            if (string.IsNullOrEmpty(storageUrl))
            {
                return null;
            }

            return new BlobContainerClient(new Uri(new Uri(storageUrl), containerName), new DefaultAzureCredential());
        }

        private static string GetAdlsGen2StoreUrl()
        {
            return Environment.GetEnvironmentVariable("dataLakeStore:storageUrl");
        }
    }
}