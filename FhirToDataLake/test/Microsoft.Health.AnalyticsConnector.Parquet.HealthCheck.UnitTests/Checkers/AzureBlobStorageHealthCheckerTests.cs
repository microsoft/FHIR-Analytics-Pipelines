// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;
using Microsoft.Health.AnalyticsConnector.HealthCheck.Checkers;
using Microsoft.Health.AnalyticsConnector.HealthCheck.Models;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.HealthCheck.UnitTests.Checkers
{
    public class AzureBlobStorageHealthCheckerTests
    {
        private static IDiagnosticLogger _diagnosticLogger = new DiagnosticLogger();
        private static ILogger<AzureBlobStorageHealthChecker> _logger = new NullLogger<AzureBlobStorageHealthChecker>();
        private readonly string _healthCheckBlobPrefix = AzureBlobStorageHealthChecker.HealthCheckBlobPrefix;
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
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new AzureBlobStorageHealthChecker(null, null, null, _diagnosticLogger, _logger));
        }

        [Fact]
        public async Task When_BlobClient_CanReadWriteABlob_HealthCheck_Succeeds()
        {
            var blobContainerClient = Substitute.For<IAzureBlobContainerClient>();

            byte[] bytes = Encoding.UTF8.GetBytes(AzureBlobStorageHealthChecker.HealthCheckUploadedContent);
            using var stream = new MemoryStream(bytes);
            blobContainerClient.UpdateBlobAsync(Arg.Any<string>(), default, cancellationToken: default).Returns("test");
            blobContainerClient.GetBlobAsync(Arg.Any<string>(), cancellationToken: default).Returns(stream);
            blobContainerClient.DeleteDirectoryIfExistsAsync(Arg.Any<string>(), cancellationToken: default).Returns(Task.CompletedTask);
            blobContainerClient.MoveDirectoryAsync(Arg.Any<string>(), Arg.Any<string>(), cancellationToken: default).Returns(Task.CompletedTask);

            var storageAccountHealthChecker = new AzureBlobStorageHealthChecker(
                new MockAzureBlobContainerClientFactory(blobContainerClient),
                Options.Create(_jobConfig),
                Options.Create(_storeConfig),
                _diagnosticLogger,
                new NullLogger<AzureBlobStorageHealthChecker>());

            HealthCheckResult result = await storageAccountHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.HEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }

        [Fact]
        public async Task When_BlobClient_ThrowExceptionWhenDeleteAndMoveDirectory_HealthCheck_Fails()
        {
            var blobContainerClient = Substitute.For<IAzureBlobContainerClient>();

            byte[] bytes = Encoding.UTF8.GetBytes(AzureBlobStorageHealthChecker.HealthCheckUploadedContent);
            using var stream = new MemoryStream(bytes);

            blobContainerClient.UpdateBlobAsync(Arg.Any<string>(), default, cancellationToken: default).Returns("test");
            blobContainerClient.GetBlobAsync(Arg.Any<string>(), cancellationToken: default).Returns(stream);
            blobContainerClient.DeleteDirectoryIfExistsAsync(Arg.Any<string>(), cancellationToken: default).ThrowsAsync<Exception>();
            var storageAccountHealthChecker = new AzureBlobStorageHealthChecker(
                new MockAzureBlobContainerClientFactory(blobContainerClient),
                Options.Create(_jobConfig),
                Options.Create(_storeConfig),
                _diagnosticLogger,
                _logger);

            HealthCheckResult result = await storageAccountHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.UNHEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }

        [Fact]
        public async Task When_BlobClient_ThrowExceptionWhenReadWriteABlob_HealthCheck_Fails()
        {
            var blobContainerClient = Substitute.For<IAzureBlobContainerClient>();

            blobContainerClient.UpdateBlobAsync(Arg.Is<string>(p => p.StartsWith(_healthCheckBlobPrefix)), default).ThrowsAsyncForAnyArgs(new Exception());
            blobContainerClient.GetBlobAsync(Arg.Is<string>(p => p.StartsWith(_healthCheckBlobPrefix))).ThrowsAsyncForAnyArgs(new Exception());
            var storageAccountHealthChecker = new AzureBlobStorageHealthChecker(
                new MockAzureBlobContainerClientFactory(blobContainerClient),
                Options.Create(_jobConfig),
                Options.Create(_storeConfig),
                _diagnosticLogger,
                _logger);

            HealthCheckResult result = await storageAccountHealthChecker.PerformHealthCheckAsync();
            Assert.Equal(HealthCheckStatus.UNHEALTHY, result.Status);
            Assert.False(result.IsCritical);
        }
    }
}
