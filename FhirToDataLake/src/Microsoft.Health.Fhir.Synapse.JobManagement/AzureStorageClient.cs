// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Core;
using Azure.Data.Tables;
using Azure.Storage.Queues;
using EnsureThat;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public class AzureStorageClient : IAzureStorageClient
    {
        private readonly ILogger<AzureStorageClient> _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureStorageClient"/> class
        /// </summary>
        /// <param name="tableUri">Table Uri.</param>
        /// <param name="tableName">Table Name.</param>
        /// <param name="queueUri">Queue Uri.</param>
        /// <param name="tokenCredential">Token Credential.</param>
        /// <param name="logger">Logger.</param>
        public AzureStorageClient(
            Uri tableUri,
            string tableName,
            Uri queueUri,
            TokenCredential tokenCredential,
            ILogger<AzureStorageClient> logger)
        {
            EnsureArg.IsNotNull(tableUri, nameof(tableUri));
            EnsureArg.IsNotNullOrWhiteSpace(tableName, nameof(tableName));
            EnsureArg.IsNotNull(queueUri, nameof(queueUri));
            EnsureArg.IsNotNull(tokenCredential, nameof(tokenCredential));
            EnsureArg.IsNotNull(logger, nameof(logger));

            AzureJobInfoTableClient = new TableClient(
                tableUri,
                tableName,
                tokenCredential);

            AzureJobMessageQueueClient = new QueueClient(
                queueUri,
                tokenCredential);

            _logger = logger;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureStorageClient"/> class for local tests
        /// since MI auth is not supported for local emulator.
        /// </summary>
        /// <param name="tableConnectionString">Table connection string.</param>
        /// <param name="tableName">Table Name.</param>
        /// <param name="queueConnectionString">Queue connection string.</param>
        /// <param name="queueName">Queue Name.</param>
        /// <param name="logger">Logger.</param>
        public AzureStorageClient(
            string tableConnectionString,
            string tableName,
            string queueConnectionString,
            string queueName,
            ILogger<AzureStorageClient> logger)
        {
            EnsureArg.IsNotNullOrWhiteSpace(tableConnectionString, nameof(tableConnectionString));
            EnsureArg.IsNotNullOrWhiteSpace(tableName, nameof(tableName));
            EnsureArg.IsNotNullOrWhiteSpace(queueConnectionString, nameof(queueConnectionString));
            EnsureArg.IsNotNullOrWhiteSpace(queueName, nameof(queueName));
            EnsureArg.IsNotNull(logger, nameof(logger));

            AzureJobInfoTableClient = new TableClient(tableConnectionString, tableName);
            AzureJobMessageQueueClient = new QueueClient(queueConnectionString, queueName);

            _logger = logger;
        }

        public TableClient AzureJobInfoTableClient { get; }

        public QueueClient AzureJobMessageQueueClient { get; }

        public bool IsInitialized()
        {
            try
            {
                AzureJobInfoTableClient.CreateIfNotExists();
                AzureJobMessageQueueClient.CreateIfNotExists();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize azure storage client.");
                return false;
            }

            _logger.LogInformation("Initialize azure storage client successfully.");
            return true;
        }
    }
}