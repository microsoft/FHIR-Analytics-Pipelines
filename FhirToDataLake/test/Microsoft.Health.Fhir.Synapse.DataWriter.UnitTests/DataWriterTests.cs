// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.UnitTests
{
    [Trait("Category", "DataWriter")]
    public class DataWriterTests
    {
        private const string LocalTestStorageUrl = "UseDevelopmentStorage=true";

        private const string TestStorageUrl = "https://fake.blob.windows.core.net";

        private const string TestContainerName = "datawritertests";

        private const string TestResourceType = "Patient";

        private readonly DateTime _testDate = new (2021, 10, 1);

        [Fact]
        public async Task GivenAValidParameter_WhenWritingToBlob_CorrectContentShouldBeWritten()
        {
            var stream = new MemoryStream();
            var bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
            await stream.WriteAsync(bytes, 0, bytes.Length);
            stream.Position = 0;

            var dataWriter = GetLocalDataWriter();
            var streamData = new StreamBatchData(stream);
            var context = GetTaskContext();
            await dataWriter.WriteAsync(streamData, context, _testDate);

            var containerClient = new AzureBlobContainerClientFactory(new NullLoggerFactory()).Create(LocalTestStorageUrl, TestContainerName);
            var blobStream = await containerClient.GetBlobAsync($"staging/mockjob/Patient/2021/10/01/Patient_mockjob_00000.parquet");
            Assert.NotNull(blobStream);

            var resultStream = new MemoryStream();
            blobStream.CopyTo(resultStream);
            Assert.Equal(bytes, resultStream.ToArray());
        }

        [Fact]
        public async Task GivenAnInvalidInputStreamData_WhenWritingToBlob_ExceptionShouldBeThrown()
        {
            var dataWriter = GetLocalDataWriter();
            var streamData = new StreamBatchData(null);
            var context = GetTaskContext();

            await Assert.ThrowsAsync<ArgumentNullException>(() => dataWriter.WriteAsync(streamData, context, _testDate));
        }

        [Fact]
        public void GivenAnInvalidBlobContainerClient_WhenCreateDataWriter_ExceptionShouldBeThrown()
        {
            Assert.Throws<AzureBlobOperationFailedException>(() => GetDataWriter());
        }

        private TaskContext GetTaskContext()
        {
            return new TaskContext(
                string.Empty,
                "mockjob",
                TestResourceType,
                DateTimeOffset.MinValue,
                DateTimeOffset.MaxValue,
                null,
                0,
                0,
                0,
                0);
        }

        private IDataSink GetDataSink()
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

        public IDataSink GetLocalDataSink()
        {
            var jobConfig = new JobConfiguration
            {
                ContainerName = TestContainerName,
            };

            var storageConfig = new DataLakeStoreConfiguration
            {
                StorageUrl = "UseDevelopmentStorage=true",
            };

            return new AzureBlobDataSink(
                Options.Create(storageConfig),
                Options.Create(jobConfig));
        }

        private AzureBlobDataWriter GetLocalDataWriter()
        {
            return new AzureBlobDataWriter(
                new AzureBlobContainerClientFactory(new NullLoggerFactory()),
                GetLocalDataSink(),
                new NullLogger<AzureBlobDataWriter>());
        }

        private AzureBlobDataWriter GetDataWriter()
        {
            return new AzureBlobDataWriter(
                new AzureBlobContainerClientFactory(new NullLoggerFactory()),
                GetDataSink(),
                new NullLogger<AzureBlobDataWriter>());
        }
    }
}
