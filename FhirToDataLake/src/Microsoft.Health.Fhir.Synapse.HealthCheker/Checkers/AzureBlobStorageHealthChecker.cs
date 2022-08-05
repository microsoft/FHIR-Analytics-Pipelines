// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.DataWriter.Azure;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Exceptions;
using Microsoft.Health.Fhir.Synapse.HealthCheker.Models;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker.Checkers
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
            : base(HealthCheckTypes.AzureBlobStorageCanReadWrite, logger)
        {
            EnsureArg.IsNotNull(azureBlobContainerClientFactory, nameof(azureBlobContainerClientFactory));
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(storeConfiguration, nameof(storeConfiguration));

            _blobContainerClient = azureBlobContainerClientFactory.Create(storeConfiguration.Value.StorageUrl, jobConfiguration.Value.ContainerName);
        }

        protected override async Task PerformHealthCheckImplAsync(HealthCheckResult healthCheckResult, CancellationToken cancellationToken)
        {
            EnsureArg.IsNotNull(healthCheckResult, nameof(healthCheckResult));

            // Ensure we can write to the storage account
            var blobPath = $"{HealthCheckBlobPrefix}";

            byte[] bytes = Encoding.UTF8.GetBytes(HealthCheckBlobPrefix);
            MemoryStream stream = new (bytes);
            await _blobContainerClient.UpdateBlobAsync(blobPath, stream, cancellationToken);

            // Ensure we can read from the storage account
            var healthCheckStream = await _blobContainerClient.GetBlobAsync(blobPath, cancellationToken: cancellationToken);
            string healthCheckContent;
            healthCheckStream.Position = 0;
            using (StreamReader reader = new (healthCheckStream, Encoding.UTF8))
            {
                healthCheckContent = reader.ReadToEnd();
            }

            if (!Equals(healthCheckContent, HealthCheckBlobPrefix))
            {
                throw new HealthCheckException("Read/Write content from blob failed.");
            }

            healthCheckResult.Status = HealthCheckStatus.PASS;
        }
    }
}
