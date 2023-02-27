// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models.AzureStorage;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;
using Microsoft.Health.AnalyticsConnector.JobManagement;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public class AzureTableCompletedBlobStore : ICompletedBlobStore
    {
        private readonly TableClient _completedBlobTableClient;
        private readonly ILogger<AzureTableCompletedBlobStore> _logger;

        private const int QueryEntityPageCount = 100;

        private bool _isInitialized;

        public AzureTableCompletedBlobStore(
            IAzureTableClientFactory azureTableClientFactory,
            IOptions<JobConfiguration> config,
            ILogger<AzureTableCompletedBlobStore> logger)
        {
            EnsureArg.IsNotNull(azureTableClientFactory, nameof(azureTableClientFactory));
            EnsureArg.IsNotNull(config, nameof(config));
            EnsureArg.IsNotNullOrWhiteSpace(config.Value.CompletedBlobTableName, nameof(config.Value.CompletedBlobTableName));

            _completedBlobTableClient = azureTableClientFactory.Create(config.Value.CompletedBlobTableName);
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));
            _isInitialized = false;
        }

        public bool IsInitialized()
        {
            if (_isInitialized)
            {
                return _isInitialized;
            }

            // try to initialize if it is not initialized yet.
            TryInitialize();
            return _isInitialized;
        }

        private void TryInitialize()
        {
            try
            {
                _completedBlobTableClient.CreateIfNotExists();
                _isInitialized = true;
                _logger.LogInformation("Initialize completed blob store successfully.");
            }
            catch (RequestFailedException ex) when (IsAuthenticationError(ex))
            {
                _logger.LogInformation(ex, "Failed to initialize completed blob store due to authentication issue.");
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Failed to initialize completed blob store.");
            }
        }

        public async Task<bool> TryAddEntityAsync(ITableEntity tableEntity, CancellationToken cancellationToken = default)
        {
            try
            {
                await _completedBlobTableClient.AddEntityAsync(tableEntity, cancellationToken);
                return true;
            }
            catch (RequestFailedException ex) when (ex.ErrorCode == AzureStorageErrorCode.AddEntityAlreadyExistsErrorCode)
            {
                _logger.LogInformation("Failed to add entity, the entity already exists.");
                return false;
            }
        }

        public async Task<List<AzureBlobInfo>> GetCompletedBlobsAsync(CancellationToken cancellationToken = default)
        {
            List<AzureBlobInfo> blobs = new List<AzureBlobInfo>();
            await foreach (Page<CompletedBlobEntity> page in _completedBlobTableClient.QueryAsync<CompletedBlobEntity>(cancellationToken: cancellationToken)
                    .AsPages(default, QueryEntityPageCount))
            {
                blobs.AddRange(page.Values.Select(item => new AzureBlobInfo(item.PartitionKey, item.RowKey)));
            }

            return blobs;
        }

        public async Task DeleteCompletedBlobTableAsync()
        {
            await _completedBlobTableClient.DeleteAsync();
        }

        private static bool IsAuthenticationError(RequestFailedException exception) =>
            string.Equals(exception.ErrorCode, AzureStorageErrorCode.NoAuthenticationInformationErrorCode, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(exception.ErrorCode, AzureStorageErrorCode.InvalidAuthenticationInfoErrorCode, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(exception.ErrorCode, AzureStorageErrorCode.AuthenticationFailedErrorCode, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(exception.ErrorCode, AzureStorageErrorCode.AuthorizationFailureErrorCode, StringComparison.OrdinalIgnoreCase);
    }
}
