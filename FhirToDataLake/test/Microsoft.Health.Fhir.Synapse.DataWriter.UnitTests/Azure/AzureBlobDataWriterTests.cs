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
using Microsoft.Health.Fhir.Synapse.Common;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions;
using NSubstitute;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.UnitTests.Azure
{
    [Trait("Category", "DataWriter")]
    public class AzureBlobDataWriterTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();

        private const string LocalTestStorageUrl = "UseDevelopmentStorage=true";

        private const string TestStorageUrl = "https://fake.blob.windows.core.net";

        private const string TestContainerName = "datawritertests";

        private const string TestResourceType = "Patient";

        private readonly DateTime _testDate = new DateTime(2021, 10, 1);

        private static readonly StorageConfiguration _storageConfiguration = new StorageConfiguration();

        [Fact]
        public async Task GivenAValidParameter_WhenWritingToBlob_CorrectContentShouldBeWritten()
        {
            var stream = new MemoryStream();
            byte[] bytes = { 1, 2, 3, 4, 5, 6 };
            await stream.WriteAsync(bytes);
            const long jobId = 1L;
            int partIndex = 1;

            IAzureBlobContainerClient containerClient = new AzureBlobContainerClientFactory(
                new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()),
                Options.Create(_storageConfiguration),
                _diagnosticLogger,
                new NullLoggerFactory())
                .Create(LocalTestStorageUrl, TestContainerName);

            // DataWriter for FHIR
            AzureBlobDataWriter dataWriter = GetLocalDataWriter();
            stream.Position = 0;
            var streamData = new StreamBatchData(stream, 1, TestResourceType);

            string dateTimeKey = _testDate.ToString("yyyy/MM/dd");
            var blobName = $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}/{TestResourceType}/{dateTimeKey}/{TestResourceType}_{partIndex:d10}.parquet";

            await dataWriter.WriteAsync(streamData, blobName);

            Stream blobStream = await containerClient.GetBlobAsync($"staging/{jobId:d20}/Patient/2021/10/01/Patient_{partIndex:d10}.parquet");
            Assert.NotNull(blobStream);

            var resultStream = new MemoryStream();
            await blobStream.CopyToAsync(resultStream);
            Assert.Equal(bytes, resultStream.ToArray());

            // DataWriter for DICOM
            dataWriter = GetLocalDataWriter(DataSourceType.DICOM);
            stream.Position = 0;
            streamData = new StreamBatchData(stream, 1, "Dicom");

            var offset = 100;
            blobName = $"{AzureStorageConstants.StagingFolderName}/{jobId:d20}/Dicom/{offset}/Dicom_{partIndex:d10}.parquet";

            await dataWriter.WriteAsync(streamData, blobName);

            blobStream = await containerClient.GetBlobAsync($"staging/{jobId:d20}/Dicom/{offset}/Dicom_{partIndex:d10}.parquet");
            Assert.NotNull(blobStream);

            resultStream = new MemoryStream();
            await blobStream.CopyToAsync(resultStream);
            Assert.Equal(bytes, resultStream.ToArray());
        }

        [Fact]
        public async Task GivenAnInvalidInputStreamData_WhenWritingToBlob_ExceptionShouldBeThrown()
        {
            AzureBlobDataWriter dataWriter = GetLocalDataWriter();
            var streamData = new StreamBatchData(null, 0, TestResourceType);

            string dateTimeKey = _testDate.ToString("yyyy/MM/dd");
            var blobName = $"{AzureStorageConstants.StagingFolderName}/{0L:d20}/{TestResourceType}/{dateTimeKey}/{TestResourceType}_{0:d10}.parquet";

            await Assert.ThrowsAsync<ArgumentNullException>(() => dataWriter.WriteAsync(streamData, blobName));
        }

        [Fact]
        public async void GivenAnInvalidBlobContainerClient_WhenCommitTheData_ExceptionShouldBeThrown()
        {
            AzureBlobDataWriter invalidContainerDataWriter = GetBrokenDataWriter();

            await Assert.ThrowsAsync<AzureBlobOperationFailedException>(() => invalidContainerDataWriter.CommitJobDataAsync(00));
        }

        [Fact]
        public async Task GivenEmptyFolder_WhenCleanJobData_ThenShouldNoExceptionBeThrown()
        {
            var blobClient = new InMemoryBlobContainerClient();

            const long jobId = 1L;
            AzureBlobDataWriter dataWriter = GetInMemoryDataWriter(blobClient);

            bool result = await dataWriter.TryCleanJobDataAsync(jobId, CancellationToken.None);
            Assert.True(result);
        }

        [Fact]
        public async Task GivenExistingJobData_WhenCleanJobData_ThenTheDataShouldBeDeleted()
        {
            var blobClient = new InMemoryBlobContainerClient();

            var stream = new MemoryStream();
            byte[] bytes = { 1, 2, 3, 4, 5, 6 };
            await stream.WriteAsync(bytes, 0, bytes.Length);
            stream.Position = 0;
            const long jobId = 1L;
            int partIndex = 1;
            AzureBlobDataWriter dataWriter = GetInMemoryDataWriter(blobClient);

            string blobName = $"staging/{jobId:d20}/Patient/2021/10/01/Patient_{partIndex:d10}.parquet";
            await blobClient.CreateBlobAsync(blobName, stream, CancellationToken.None);
            bool blobExists = await blobClient.BlobExistsAsync(blobName, CancellationToken.None);
            Assert.True(blobExists);

            bool result = await dataWriter.TryCleanJobDataAsync(jobId, CancellationToken.None);
            Assert.True(result);
            blobExists = await blobClient.BlobExistsAsync(blobName, CancellationToken.None);
            Assert.False(blobExists);
        }

        [Fact]
        public async Task GivenValidData_WhenCommitData_ThenTheDataShouldBeCommitted()
        {
            var blobClient = new InMemoryBlobContainerClient();

            var stream = new MemoryStream();
            byte[] bytes = { 1, 2, 3, 4, 5, 6 };
            await stream.WriteAsync(bytes);
            const long jobId = 1L;
            int partIndex = 1;

            // DataWriter for FHIR
            AzureBlobDataWriter dataWriter = GetInMemoryDataWriter(blobClient);

            stream.Position = 0;
            string blobName = $"staging/{jobId:d20}/Patient/2021/10/01/Patient_{partIndex:d10}.parquet";
            await blobClient.CreateBlobAsync(blobName, stream, CancellationToken.None);
            Assert.True(await blobClient.BlobExistsAsync(blobName, CancellationToken.None));

            Assert.Null(await Record.ExceptionAsync(async () => await dataWriter.CommitJobDataAsync(jobId, CancellationToken.None)));
            Assert.False(await blobClient.BlobExistsAsync(blobName, CancellationToken.None));

            string expectedBlobName = $"result/Patient/2021/10/01/{jobId:d20}/Patient_{partIndex:d10}.parquet";
            Assert.True(await blobClient.BlobExistsAsync(expectedBlobName, CancellationToken.None));

            // DataWriter for DICOM
            dataWriter = GetInMemoryDataWriter(blobClient, DataSourceType.DICOM);

            stream.Position = 0;
            blobName = $"staging/{jobId:d20}/Dicom/100/Dicom_{partIndex:d10}.parquet";
            await blobClient.CreateBlobAsync(blobName, stream, CancellationToken.None);
            Assert.True(await blobClient.BlobExistsAsync(blobName, CancellationToken.None));

            Assert.Null(await Record.ExceptionAsync(async () => await dataWriter.CommitJobDataAsync(jobId, CancellationToken.None)));
            Assert.False(await blobClient.BlobExistsAsync(blobName, CancellationToken.None));

            expectedBlobName = $"result/Dicom/100/{jobId:d20}/Dicom_{partIndex:d10}.parquet";
            Assert.True(await blobClient.BlobExistsAsync(expectedBlobName, CancellationToken.None));
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

        private static AzureBlobDataWriter GetLocalDataWriter(DataSourceType dataSourceType = DataSourceType.FHIR)
        {
            return new AzureBlobDataWriter(
                Options.Create(new DataSourceConfiguration { Type = dataSourceType }),
                new AzureBlobContainerClientFactory(
                    new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()),
                    Options.Create(_storageConfiguration),
                    _diagnosticLogger,
                    new NullLoggerFactory()),
                GetLocalDataSink(),
                new NullLogger<AzureBlobDataWriter>());
        }

        private static AzureBlobDataWriter GetInMemoryDataWriter(IAzureBlobContainerClient blobClient, DataSourceType dataSourceType = DataSourceType.FHIR)
        {
            var mockFactory = Substitute.For<IAzureBlobContainerClientFactory>();
            mockFactory.Create(Arg.Any<string>(), Arg.Any<string>()).ReturnsForAnyArgs(blobClient);

            return new AzureBlobDataWriter(
                Options.Create(new DataSourceConfiguration { Type = dataSourceType }),
                mockFactory,
                GetLocalDataSink(),
                new NullLogger<AzureBlobDataWriter>());
        }

        private static AzureBlobDataWriter GetBrokenDataWriter()
        {
            return new AzureBlobDataWriter(
                Options.Create(new DataSourceConfiguration()),
                new AzureBlobContainerClientFactory(
                    new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()),
                    Options.Create(_storageConfiguration),
                    _diagnosticLogger,
                    new NullLoggerFactory()),
                GetBrokenDataSink(),
                new NullLogger<AzureBlobDataWriter>());
        }
    }
}