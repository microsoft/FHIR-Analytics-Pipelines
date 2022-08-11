// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.UnitTests.Checkers
{
    public class AzureBlobStorageHealthCheckerTests
    {
        private readonly IAzureBlobContainerClient _blobContainerClient;
        private readonly string _healthCheckBlobPrefix = AzureBlobStorageHealthChecker.HealthCheckBlobPrefix;
        private AzureBlobStorageHealthChecker _storageAccountHealthChecker;
        private JobConfiguration _jobConfig;
        private DataLakeStoreConfiguration _storeConfig;

        public AzureBlobStorageHealthCheckerTests()
        {
            _jobConfig = new JobConfiguration()
            {
                ContainerName = "test",
            };

            _storeConfig = new DataLakeStoreConfiguration()
            {
                StorageUrl = "test",
            };
            _blobContainerClient = Substitute.For<IAzureBlobContainerClient>();
        }

        [Fact]
        public async Task When_BlobClient_CanReadWriteABlob_HealthCheck_Succeeds()
        {
            byte[] bytes = Encoding.UTF8.GetBytes(AzureBlobStorageHealthChecker.HealthCheckBlobPrefix);
            using MemoryStream stream = new (bytes);
            _blobContainerClient.UpdateBlobAsync(Arg.Is<string>(p => p.StartsWith(_healthCheckBlobPrefix)), default, cancellationToken: default).Returns("test");
            _blobContainerClient.GetBlobAsync(Arg.Is<string>(p => p.StartsWith(_healthCheckBlobPrefix)), cancellationToken: default).Returns(stream);
            _storageAccountHealthChecker = new AzureBlobStorageHealthChecker(
                new MockAzureBlobContainerClientFactory(_blobContainerClient),
                Options.Create(_jobConfig),
                Options.Create(_storeConfig),
                new NullLogger<AzureBlobStorageHealthChecker>());

            var result = await _storageAccountHealthChecker.PerformHealthCheckAsync(default);
            Assert.Equal(HealthCheckStatus.PASS, result.Status);
            Assert.False(result.IsCritical);
        }

        [Fact]
        public async Task When_BlobClient_ThrowExceptionWhenReadWriteABlob_HealthCheck_Fails()
        {
            byte[] bytes = Encoding.UTF8.GetBytes(AzureBlobStorageHealthChecker.HealthCheckBlobPrefix);
            using MemoryStream stream = new (bytes);

            _blobContainerClient.UpdateBlobAsync(Arg.Is<string>(p => p.StartsWith(_healthCheckBlobPrefix)), default, default).ThrowsAsyncForAnyArgs(new Exception());
            _blobContainerClient.GetBlobAsync(Arg.Is<string>(p => p.StartsWith(_healthCheckBlobPrefix)), default).ThrowsAsyncForAnyArgs(new Exception());
            _storageAccountHealthChecker = new AzureBlobStorageHealthChecker(
                new MockAzureBlobContainerClientFactory(_blobContainerClient),
                Options.Create(_jobConfig),
                Options.Create(_storeConfig),
                new NullLogger<AzureBlobStorageHealthChecker>());

            var result = await _storageAccountHealthChecker.PerformHealthCheckAsync(default);
            Assert.Equal(HealthCheckStatus.FAIL, result.Status);
            Assert.False(result.IsCritical);
        }
    }
}
