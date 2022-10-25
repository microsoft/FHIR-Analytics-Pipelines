// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public class AzureBlobStorageHealthChecker : BaseHealthChecker
    {
        public const string HealthCheckBlobPrefix = ".healthcheck";
        public const string HealthCheckSourceSubFolder = "__healthcheck__/source";
        public const string HealthCheckTargetSubFolder = "__healthcheck__/target";
        public const string HealthCheckUploadedContent = "healthCheckContent";
        private readonly IAzureBlobContainerClient _blobContainerClient;

        public AzureBlobStorageHealthChecker(
            IAzureBlobContainerClientFactory azureBlobContainerClientFactory,
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<DataLakeStoreConfiguration> storeConfiguration,
            IDiagnosticLogger diagnosticLogger,
            ILogger<AzureBlobStorageHealthChecker> logger)
            : base(HealthCheckTypes.AzureBlobStorageCanReadWrite, false, diagnosticLogger, logger)
        {
            EnsureArg.IsNotNull(azureBlobContainerClientFactory, nameof(azureBlobContainerClientFactory));
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(storeConfiguration, nameof(storeConfiguration));

            _blobContainerClient = azureBlobContainerClientFactory.Create(storeConfiguration.Value.StorageUrl, jobConfiguration.Value.ContainerName);
        }

        protected override async Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            var healthCheckResult = new HealthCheckResult(HealthCheckTypes.AzureBlobStorageCanReadWrite);
            var blobPath = $"{HealthCheckSourceSubFolder}/{HealthCheckBlobPrefix}";

            try
            {
                // Ensure we can write to the storage account
                await UploadToBlobAsync(HealthCheckUploadedContent, blobPath, cancellationToken);
            }
            catch (Exception e)
            {
                _logger.LogInformation(e, $"Check write storage account failed: {e}.");

                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = $"Write content to blob failed. {e.Message}";
                return healthCheckResult;
            }

            try
            {
                // Ensure we can read from the storage account
                string result = await DownloadFromBlobAsync(blobPath, cancellationToken);
                if (!Equals(result, HealthCheckUploadedContent))
                {
                    healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                    healthCheckResult.ErrorMessage = "Read/Write content from blob failed.";
                    return healthCheckResult;
                }
            }
            catch (Exception e)
            {
                _logger.LogInformation(e, $"Check read storage account failed: {e}.");

                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = $"Read content from blob failed. {e.Message}";
                return healthCheckResult;
            }

            try
            {
                // Ensure hierachical namespace is enabled (supports directory operations) from the storage account
                await _blobContainerClient.DeleteDirectoryIfExistsAsync(HealthCheckTargetSubFolder, cancellationToken);
                await _blobContainerClient.MoveDirectoryAsync(HealthCheckSourceSubFolder, HealthCheckTargetSubFolder, cancellationToken);
            }
            catch (Exception e)
            {
                _logger.LogInformation(e, $"Check hierachical namespace enabled in storage account failed: {e}.");

                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = $"Check hierachical namespace enabled in storage account failed. {e.Message}";
                return healthCheckResult;
            }

            healthCheckResult.Status = HealthCheckStatus.HEALTHY;
            return healthCheckResult;
        }

        private async Task UploadToBlobAsync(string uploadedContent, string blobPath, CancellationToken cancellationToken)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(uploadedContent);
            using MemoryStream stream = new (bytes);
            await _blobContainerClient.UpdateBlobAsync(blobPath, stream, cancellationToken);
        }

        private async Task<string> DownloadFromBlobAsync(string blobPath, CancellationToken cancellationToken)
        {
            using var resultStream = await _blobContainerClient.GetBlobAsync(blobPath, cancellationToken: cancellationToken);
            resultStream.Position = 0;
            using StreamReader reader = new (resultStream, Encoding.UTF8);
            return reader.ReadToEnd();
        }
    }
}
