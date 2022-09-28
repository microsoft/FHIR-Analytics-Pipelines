// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.UnitTests
{
    [Trait("Category", "DataWriter")]
    public class DataWriterTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        private const string LocalTestStorageUrl = "UseDevelopmentStorage=true";

        private const string TestStorageUrl = "https://fake.blob.windows.core.net";

        private const string TestContainerName = "datawritertests";

        private const string TestResourceType = "Patient";

        private readonly DateTime _testDate = new (2021, 10, 1);

        private static readonly StorageConfiguration _storageConfiguration = new ();

        [Fact]
        public async Task GivenAValidParameter_WhenWritingToBlob_CorrectContentShouldBeWritten()
        {
            var stream = new MemoryStream();
            var bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
            await stream.WriteAsync(bytes, 0, bytes.Length);
            stream.Position = 0;
            const long jobId = 1L;
            var partIndex = 1;
            var dataWriter = GetLocalDataWriter();
            var streamData = new StreamBatchData(stream, 1, TestResourceType);
            await dataWriter.WriteAsync(streamData, jobId, partIndex, _testDate);

            var containerClient = new AzureBlobContainerClientFactory(new DefaultTokenCredentialProvider(_diagnosticLogger, new NullLogger<DefaultTokenCredentialProvider>()), Options.Create(_storageConfiguration), _diagnosticLogger, new NullLoggerFactory()).Create(LocalTestStorageUrl, TestContainerName);
            var blobStream = await containerClient.GetBlobAsync($"staging/{jobId:d20}/Patient/2021/10/01/Patient_{partIndex:d10}.parquet");
            Assert.NotNull(blobStream);

            var resultStream = new MemoryStream();
            await blobStream.CopyToAsync(resultStream);
            Assert.Equal(bytes, resultStream.ToArray());
        }

        [Fact]
        public async Task GivenAnInvalidInputStreamData_WhenWritingToBlob_ExceptionShouldBeThrown()
        {
            var dataWriter = GetLocalDataWriter();
            var streamData = new StreamBatchData(null, 0, TestResourceType);
            await Assert.ThrowsAsync<ArgumentNullException>(() => dataWriter.WriteAsync(streamData, 0L, 0, _testDate));
        }

        [Fact]
        public void GivenAnInvalidBlobContainerClient_WhenCreateDataWriter_ExceptionShouldBeThrown()
        {
            Assert.Throws<AzureBlobOperationFailedException>(GetBrokenDataWriter);
        }

        [Fact]
        public async Task GivenEmptyFolder_WhenCleanJobData_ThenShouldNoExceptionBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();

            const long jobId = 1L;
            var dataWriter = GetInMemoryDataWriter(blobClient);

            var result = await dataWriter.TryCleanJobDataAsync(jobId, CancellationToken.None);
            Assert.True(result);
        }

        [Fact]
        public async Task GivenExistingJobData_WhenCleanJobData_ThenTheDataShouldBeDeleted()
        {
            var blobClient = new InMemoryBlobContainerClient();

            var stream = new MemoryStream();
            var bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
            await stream.WriteAsync(bytes, 0, bytes.Length);
            stream.Position = 0;
            const long jobId = 1L;
            var partIndex = 1;
            var dataWriter = GetInMemoryDataWriter(blobClient);

            var blobName = $"staging/{jobId:d20}/Patient/2021/10/01/Patient_{partIndex:d10}.parquet";
            await blobClient.CreateBlobAsync(blobName, stream, CancellationToken.None);
            var blobExists = await blobClient.BlobExistsAsync(blobName, CancellationToken.None);
            Assert.True(blobExists);

            var result = await dataWriter.TryCleanJobDataAsync(jobId, CancellationToken.None);
            Assert.True(result);
            blobExists = await blobClient.BlobExistsAsync(blobName, CancellationToken.None);
            Assert.False(blobExists);
        }

        [Fact]
        public async Task GivenValidData_WhenCommitData_ThenTheDataShouldBeCommitted()
        {
            var blobClient = new InMemoryBlobContainerClient();

            var stream = new MemoryStream();
            var bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
            await stream.WriteAsync(bytes, 0, bytes.Length);
            stream.Position = 0;
            const long jobId = 1L;
            var partIndex = 1;
            var dataWriter = GetInMemoryDataWriter(blobClient);

            var blobName = $"staging/{jobId:d20}/Patient/2021/10/01/Patient_{partIndex:d10}.parquet";
            await blobClient.CreateBlobAsync(blobName, stream, CancellationToken.None);
            var blobExists = await blobClient.BlobExistsAsync(blobName, CancellationToken.None);
            Assert.True(blobExists);

            var exception = await Record.ExceptionAsync(async () => await dataWriter.CommitJobDataAsync(jobId, CancellationToken.None));
            Assert.Null(exception);
            blobExists = await blobClient.BlobExistsAsync(blobName, CancellationToken.None);
            Assert.False(blobExists);
            var expectedBlobName = $"result/Patient/2021/10/01/{jobId:d20}/Patient_{partIndex:d10}.parquet";
            blobExists = await blobClient.BlobExistsAsync(expectedBlobName, CancellationToken.None);
            Assert.True(blobExists);
        }

        private static IDataSink GetBrokenDataSink()
        {
            var jobConfig = new JobConfiguration
            {
                ContainerName = TestContainerName,
            };

            var storageConfig = new DataLakeStoreConfiguration
            {
                StorageUrl = TestStorageUrl,
            };

            return new AzureBlobDataSink(
                Options.Create(storageConfig),
                Options.Create(jobConfig));
        }

        private static IDataSink GetLocalDataSink()
        {
            var jobConfig = new JobConfiguration
            {
                ContainerName = TestContainerName,
            };

            var storageConfig = new DataLakeStoreConfiguration
            {
                StorageUrl = LocalTestStorageUrl,
            };

            return new AzureBlobDataSink(
                Options.Create(storageConfig),
                Options.Create(jobConfig));
        }

        private static AzureBlobDataWriter GetLocalDataWriter()
        {
            return new AzureBlobDataWriter(
                new AzureBlobContainerClientFactory(
                    new DefaultTokenCredentialProvider(_diagnosticLogger, new NullLogger<DefaultTokenCredentialProvider>()),
                    Options.Create(_storageConfiguration),
                    _diagnosticLogger,
                    new NullLoggerFactory()),
                GetLocalDataSink(),
                _diagnosticLogger,
                new NullLogger<AzureBlobDataWriter>());
        }

        private static AzureBlobDataWriter GetInMemoryDataWriter(IAzureBlobContainerClient blobClient)
        {
            var mockFactory = Substitute.For<IAzureBlobContainerClientFactory>();
            mockFactory.Create(Arg.Any<string>(), Arg.Any<string>()).ReturnsForAnyArgs(blobClient);

            return new AzureBlobDataWriter(
                mockFactory,
                GetLocalDataSink(),
                _diagnosticLogger,
                new NullLogger<AzureBlobDataWriter>());
        }

        private static AzureBlobDataWriter GetBrokenDataWriter()
        {
            return new AzureBlobDataWriter(
                new AzureBlobContainerClientFactory(
                    new DefaultTokenCredentialProvider(_diagnosticLogger, new NullLogger<DefaultTokenCredentialProvider>()),
                    Options.Create(_storageConfiguration),
                    _diagnosticLogger,
                    new NullLoggerFactory()),
                GetBrokenDataSink(),
                _diagnosticLogger,
                new NullLogger<AzureBlobDataWriter>());
        }
    }
}