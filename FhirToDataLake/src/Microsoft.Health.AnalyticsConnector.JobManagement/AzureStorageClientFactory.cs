// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Core;
using Azure.Data.Tables;
using Azure.Storage.Queues;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Authentication;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;

namespace Microsoft.Health.AnalyticsConnector.JobManagement
{
    public class AzureStorageClientFactory : IAzureStorageClientFactory
    {
        private readonly ITokenCredentialProvider _credentialProvider;
        private readonly ILogger<AzureStorageClientFactory> _logger;

        private readonly string _tableUrl;
        private readonly string _tableName;
        private readonly string _queueUrl;
        private readonly string _queueName;
        private readonly string? _internalConnectionString;

        public AzureStorageClientFactory(
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<StorageConfiguration> storageConfiguration,
            ITokenCredentialProvider credentialProvider,
            ILogger<AzureStorageClientFactory> logger)
        {
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(storageConfiguration, nameof(storageConfiguration));
            _queueName = EnsureArg.IsNotNullOrWhiteSpace(jobConfiguration.Value.JobInfoQueueName, nameof(jobConfiguration.Value.JobInfoQueueName));
            _tableName = EnsureArg.IsNotNullOrWhiteSpace(jobConfiguration.Value.JobInfoTableName, nameof(jobConfiguration.Value.JobInfoTableName));
            _internalConnectionString = storageConfiguration.Value.InternalStorageConnectionString;
            _credentialProvider = EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            if (string.IsNullOrWhiteSpace(_internalConnectionString))
            {
                _tableUrl = EnsureArg.IsNotNullOrWhiteSpace(jobConfiguration.Value.TableUrl, nameof(jobConfiguration.Value.TableUrl));
                EnsureArg.IsNotNullOrWhiteSpace(jobConfiguration.Value.QueueUrl, nameof(jobConfiguration.Value.QueueUrl));

                _queueUrl = jobConfiguration.Value.QueueUrl;

                if (!IsStorageEmulatorConnectionString(_queueUrl) && !_queueUrl.EndsWith("/"))
                {
                    // If the baseUri has relative parts (like /api), then the relative part must be terminated with a slash (like /api/).
                    // Otherwise the relative part will be omitted when creating new Uri with queue name. See https://docs.microsoft.com/en-us/dotnet/api/system.uri.-ctor?view=net-6.0
                    _queueUrl += "/";
                }
            }
            else
            {
                _tableUrl = string.Empty;
                _queueUrl = string.Empty;
            }
        }

        // For local test
        public AzureStorageClientFactory(
            string tableName,
            string queueName,
            ITokenCredentialProvider credentialProvider,
            ILogger<AzureStorageClientFactory> logger)
        {
            EnsureArg.IsNotNullOrWhiteSpace(tableName, nameof(tableName));
            EnsureArg.IsNotNullOrWhiteSpace(queueName, nameof(queueName));
            _credentialProvider = EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
            _logger = EnsureArg.IsNotNull(logger, nameof(logger));

            _tableUrl = ConfigurationConstants.StorageEmulatorConnectionString;
            _tableName = tableName;
            _queueUrl = ConfigurationConstants.StorageEmulatorConnectionString;
            _queueName = queueName;
        }

        public TableClient CreateTableClient()
        {
            // Create client for local emulator.
            if (IsStorageEmulatorConnectionString(_tableUrl))
            {
                _logger.LogInformation("Initializes azure storage table client using storage emulator connection string.");
                return new TableClient(_tableUrl, _tableName);
            }

            if (!string.IsNullOrWhiteSpace(_internalConnectionString))
            {
                _logger.LogInformation($"Initializes azure storage table client using internal connection string {_internalConnectionString}.");
                return new TableClient(_internalConnectionString, _tableName);
            }

            var tableUri = new Uri(_tableUrl);
            TokenCredential? tokenCredential = _credentialProvider.GetCredential(TokenCredentialTypes.Internal);

            _logger.LogInformation($"Initializes azure storage table client using the specified URL {tableUri} and token credential.");
            return new TableClient(
                tableUri,
                _tableName,
                tokenCredential);
        }

        public QueueClient CreateQueueClient()
        {
            // Create client for local emulator.
            if (IsStorageEmulatorConnectionString(_queueUrl))
            {
                _logger.LogInformation("Initializes azure storage queue client using storage emulator connection string.");
                return new QueueClient(_queueUrl, _queueName);
            }

            if (!string.IsNullOrWhiteSpace(_internalConnectionString))
            {
                _logger.LogInformation($"Initializes azure storage queue client using internal connection string {_internalConnectionString}.");
                return new QueueClient(_internalConnectionString, _queueName);
            }

            var queueUri = new Uri($"{_queueUrl}{_queueName}");
            TokenCredential? tokenCredential = _credentialProvider.GetCredential(TokenCredentialTypes.Internal);

            _logger.LogInformation($"Initializes azure storage queue client using the specified URL {queueUri} and token credential.");
            return new QueueClient(
                queueUri,
                tokenCredential);
        }

        private static bool IsStorageEmulatorConnectionString(string str) => string.Equals(
            str,
            ConfigurationConstants.StorageEmulatorConnectionString,
            StringComparison.OrdinalIgnoreCase);
    }
}