// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure.Core;
using Azure.Data.Tables;
using EnsureThat;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class AzureTableClientFactory : IAzureTableClientFactory
    {
        private const string StorageEmulatorConnectionString = "UseDevelopmentStorage=true";
        private readonly TokenCredential _tokenCredential;

        private readonly string _tableUrl;
        private readonly string _tableName;

        public AzureTableClientFactory(
            IOptions<JobConfiguration> config,
            ITokenCredentialProvider credentialProvider)
        {
            EnsureArg.IsNotNull(config, nameof(config));
            EnsureArg.IsNotNullOrWhiteSpace(config.Value.TableUrl, nameof(config.Value.TableUrl));
            EnsureArg.IsNotNullOrWhiteSpace(config.Value.AgentName, nameof(config.Value.AgentName));

            // If the baseUri has relative parts (like /api), then the relative part must be terminated with a slash (like /api/).
            // Otherwise the relative part will be omitted when creating new search Uris. See https://docs.microsoft.com/en-us/dotnet/api/system.uri.-ctor?view=net-6.0
            _tableUrl = config.Value.TableUrl.EndsWith("/") ? config.Value.TableUrl : $"{config.Value.TableUrl}/";
            _tableName = JobKeyProvider.MetadataTableName(config.Value.AgentName);

            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
            _tokenCredential = credentialProvider.GetCredential(TokenCredentialTypes.Internal);
        }

        public AzureTableClientFactory(
            string tableName,
            ITokenCredentialProvider credentialProvider)
        {
            EnsureArg.IsNotNullOrWhiteSpace(tableName, nameof(tableName));
            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));

            _tableUrl = StorageEmulatorConnectionString;
            _tableName = tableName;
            _tokenCredential = credentialProvider.GetCredential(TokenCredentialTypes.Internal);

        }

        public TableClient Create()
        {
            // Create client for local emulator.
            if (string.Equals(_tableUrl, StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return new TableClient(_tableUrl, _tableName);
            }

            var tableUri = new Uri(_tableUrl);

            return new TableClient(
                tableUri,
                _tableName,
                _tokenCredential);
        }
    }
}