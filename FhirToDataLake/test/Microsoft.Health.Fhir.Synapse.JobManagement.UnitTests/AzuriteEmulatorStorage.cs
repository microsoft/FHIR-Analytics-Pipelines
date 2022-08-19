// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.UnitTests
{
    public class AzuriteEmulatorStorage : IStorage
    {
        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";

        public AzuriteEmulatorStorage(string agentName)
        {
            EnsureArg.IsNotNull(agentName, nameof(agentName));

            TableUrl = StorageEmulatorConnectionString;
            TableName = AzureStorageKeyProvider.JobInfoTableName(agentName);

            QueueUrl = StorageEmulatorConnectionString;
            QueueName = AzureStorageKeyProvider.JobMessageQueueName(agentName);

            UseConnectionString = true;
        }

        public string TableUrl { get; }

        public string TableName { get; }

        public string QueueUrl { get; }

        public string QueueName { get; }

        public bool UseConnectionString { get; }
    }
}