// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Exceptions;
using Microsoft.Health.Fhir.Synapse.HealthCheck.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public class AzureBlobStorageHealthChecker : BaseHealthChecker
    {
        public const string HealthCheckBlobPrefix = ".healthcheck";
        private readonly IAzureBlobContainerClient _blobContainerClient;

        public AzureBlobStorageHealthChecker(
            IAzureBlobContainerClientFactory azureBlobContainerClientFactory,
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<DataLakeStoreConfiguration> storeConfiguration,
            ILogger<AzureBlobStorageHealthChecker> logger)
            : base(HealthCheckTypes.AzureBlobStorageCanReadWrite, false, logger)
        {
            EnsureArg.IsNotNull(azureBlobContainerClientFactory, nameof(azureBlobContainerClientFactory));
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(storeConfiguration, nameof(storeConfiguration));

            _blobContainerClient = azureBlobContainerClientFactory.Create(storeConfiguration.Value.StorageUrl, jobConfiguration.Value.ContainerName);
        }

        protected override async Task<HealthCheckResult> PerformHealthCheckImplAsync(CancellationToken cancellationToken)
        {
            var healthCheckResult = new HealthCheckResult(HealthCheckTypes.AzureBlobStorageCanReadWrite, false);
            var blobPath = $"{HealthCheckBlobPrefix}";

            try
            {
                // Ensure we can write to the storage account
                byte[] bytes = Encoding.UTF8.GetBytes(HealthCheckBlobPrefix);
                using MemoryStream stream = new (bytes);
                await _blobContainerClient.UpdateBlobAsync(blobPath, stream, cancellationToken);
            }
            catch (Exception e)
            {
                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = "Write content to blob failed." + e.Message;
                return healthCheckResult;
            }

            try
            {
                // Ensure we can read from the storage account
                var healthCheckStream = await _blobContainerClient.GetBlobAsync(blobPath, cancellationToken: cancellationToken);
                string healthCheckContent;
                healthCheckStream.Position = 0;
                using StreamReader reader = new (healthCheckStream, Encoding.UTF8);
                healthCheckContent = reader.ReadToEnd();

                if (!Equals(healthCheckContent, HealthCheckBlobPrefix))
                {
                    healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                    healthCheckResult.ErrorMessage = "Read/Write content from blob failed.";
                    return healthCheckResult;
                }
            }
            catch (Exception e)
            {
                healthCheckResult.Status = HealthCheckStatus.UNHEALTHY;
                healthCheckResult.ErrorMessage = "Read content from blob failed." + e.Message;
                return healthCheckResult;
            }

            healthCheckResult.Status = HealthCheckStatus.HEALTHY;
            return healthCheckResult;
        }
    }
}
