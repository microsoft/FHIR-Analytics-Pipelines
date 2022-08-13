// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public class AzureStorage : IStorage
    {
        public AzureStorage(IOptions<JobConfiguration> config)
        {
            EnsureArg.IsNotNull(config, nameof(config));
            EnsureArg.IsNotNullOrEmpty(config.Value.TableUrl, nameof(config.Value.TableUrl));
            EnsureArg.IsNotNullOrEmpty(config.Value.QueueUrl, nameof(config.Value.QueueUrl));
            EnsureArg.IsNotNullOrEmpty(config.Value.AgentName, nameof(config.Value.AgentName));

            // If the baseUri has relative parts (like /api), then the relative part must be terminated with a slash (like /api/).
            // Otherwise the relative part will be omitted when creating new search Uris. See https://docs.microsoft.com/en-us/dotnet/api/system.uri.-ctor?view=net-6.0
            TableUrl = !config.Value.TableUrl.EndsWith("/") ? $"{config.Value.TableUrl}/" : config.Value.TableUrl;
            TableName = AzureStorageKeyProvider.JobInfoTableName(config.Value.AgentName);

            QueueUrl = !config.Value.QueueUrl.EndsWith("/") ? $"{config.Value.QueueUrl}/" : config.Value.QueueUrl;
            QueueName = AzureStorageKeyProvider.JobMessageQueueName(config.Value.AgentName);

            UseConnectionString = false;
        }

        public string TableUrl { get; }

        public string TableName { get; }

        public string QueueUrl { get; }

        public string QueueName { get; }

        public bool UseConnectionString { get; }
    }
}