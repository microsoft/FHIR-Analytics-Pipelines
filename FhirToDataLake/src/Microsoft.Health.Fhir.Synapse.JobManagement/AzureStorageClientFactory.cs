// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Data.Tables;
using Azure.Storage.Queues;
using EnsureThat;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public class AzureStorageClientFactory : IAzureStorageClientFactory
    {
        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";
        private readonly ITokenCredentialProvider _credentialProvider;

        private readonly string _tableUrl;
        private readonly string _tableName;
        private readonly string _queueUrl;
        private readonly string _queueName;
        private readonly string _internalConnectionString;

        public AzureStorageClientFactory(
            IOptions<JobConfiguration> jobConfiguration,
            IOptions<StorageConfiguration> storageConfiguration,
            ITokenCredentialProvider credentialProvider)
        {
            EnsureArg.IsNotNull(jobConfiguration, nameof(jobConfiguration));
            EnsureArg.IsNotNull(storageConfiguration, nameof(storageConfiguration));
            _tableUrl = EnsureArg.IsNotNullOrWhiteSpace(jobConfiguration.Value.TableUrl, nameof(jobConfiguration.Value.TableUrl));
            EnsureArg.IsNotNullOrWhiteSpace(jobConfiguration.Value.QueueUrl, nameof(jobConfiguration.Value.QueueUrl));
            EnsureArg.IsNotNullOrWhiteSpace(jobConfiguration.Value.AgentName, nameof(jobConfiguration.Value.AgentName));

            _tableName = AzureStorageKeyProvider.JobInfoTableName(jobConfiguration.Value.AgentName);
            _internalConnectionString = storageConfiguration.Value.InternalStorageConnectionString;

            if (jobConfiguration.Value.QueueUrl != StorageEmulatorConnectionString)
            {
                // If the baseUri has relative parts (like /api), then the relative part must be terminated with a slash (like /api/).
                // Otherwise the relative part will be omitted when creating new Uri with queue name. See https://docs.microsoft.com/en-us/dotnet/api/system.uri.-ctor?view=net-6.0
            _queueUrl = jobConfiguration.Value.QueueUrl.EndsWith("/") ? jobConfiguration.Value.QueueUrl : $"{jobConfiguration.Value.QueueUrl}/";
            }
            else
            {
                _queueUrl = jobConfiguration.Value.QueueUrl;
            }

            _queueName = AzureStorageKeyProvider.JobMessageQueueName(jobConfiguration.Value.AgentName);

            _credentialProvider = EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
        }

        public AzureStorageClientFactory(
            string tableName,
            string queueName,
            ITokenCredentialProvider credentialProvider)
        {
            EnsureArg.IsNotNullOrWhiteSpace(tableName, nameof(tableName));
            EnsureArg.IsNotNullOrWhiteSpace(queueName, nameof(queueName));
            _credentialProvider = EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));

            _tableUrl = StorageEmulatorConnectionString;
            _tableName = tableName;
            _queueUrl = StorageEmulatorConnectionString;
            _queueName = queueName;
        }

        public TableClient CreateTableClient()
        {
            // Create client for local emulator.
            if (string.Equals(_tableUrl, StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return new TableClient(_tableUrl, _tableName);
            }

            if (!string.IsNullOrEmpty(_internalConnectionString))
            {
                return new TableClient(_internalConnectionString, _tableName);
            }

            var tableUri = new Uri(_tableUrl);
            var tokenCredential = _credentialProvider.GetCredential(TokenCredentialTypes.Internal);

            return new TableClient(
                tableUri,
                _tableName,
                tokenCredential);
        }

        public QueueClient CreateQueueClient()
        {
            // Create client for local emulator.
            if (string.Equals(_queueUrl, StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return new QueueClient(_queueUrl, _queueName);
            }

            if (!string.IsNullOrEmpty(_internalConnectionString))
            {
                return new QueueClient(_internalConnectionString, _queueName);
            }

            var queueUri = new Uri($"{_queueUrl}{_queueName}");
            var tokenCredential = _credentialProvider.GetCredential(TokenCredentialTypes.Internal);

            return new QueueClient(
                queueUri,
                tokenCredential);
        }
    }
}