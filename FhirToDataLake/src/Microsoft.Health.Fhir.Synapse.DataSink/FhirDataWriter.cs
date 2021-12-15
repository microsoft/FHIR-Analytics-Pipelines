// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Azure;
using Microsoft.Health.Fhir.Synapse.Azure.Blob;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;

namespace Microsoft.Health.Fhir.Synapse.DataSink
{
    public class FhirDataWriter : IFhirDataWriter
    {
        // Date format in blob path.
        private const string DateKeyFormat = "yyyy/MM/dd";

        private readonly IAzureBlobContainerClient _containerClient;
        private readonly ILogger<FhirDataWriter> _logger;

        public FhirDataWriter(
            IAzureBlobContainerClientFactory containerClientFactory,
            IFhirDataSink dataSink,
            ILogger<FhirDataWriter> logger)
        {
            EnsureArg.IsNotNull(containerClientFactory, nameof(containerClientFactory));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _containerClient = containerClientFactory.Create(dataSink.StorageUrl, dataSink.Location);
            _logger = logger;
        }

        public async Task<string> WriteAsync(
            StreamBatchData inputData,
            TaskContext context,
            DateTime dateTime,
            CancellationToken cancellationToken = default)
        {
            EnsureArg.IsNotNull(inputData, nameof(inputData));
            EnsureArg.IsNotNull(context, nameof(context));

            var blobName = GetDataFileName(dateTime, context.ResourceType, context.JobId, context.PartId);
            var blobUrl = await _containerClient.UpdateBlobAsync(blobName, inputData.Value, cancellationToken);

            _logger.LogInformation($"Write stream batch data to {blobUrl} successfully.");

            return blobUrl;
        }

        private string GetDataFileName(
            DateTime dateTime,
            string resourceType,
            string jobId,
            int partId)
        {
            var dateTimeKey = dateTime.ToString(DateKeyFormat);

            return $"{AzureStorageConstants.StagingFolderName}/{jobId}/{resourceType}/{dateTimeKey}/{resourceType}_{jobId}_{partId:d5}.parquet";
        }
    }
}
