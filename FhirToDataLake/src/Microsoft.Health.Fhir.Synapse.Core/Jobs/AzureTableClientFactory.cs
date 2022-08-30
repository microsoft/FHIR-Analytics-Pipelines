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

        public AzureTableClientFactory(
            IOptions<JobConfiguration> config,
            ITokenCredentialProvider credentialProvider)
        {
            EnsureArg.IsNotNull(config, nameof(config));
            EnsureArg.IsNotNullOrWhiteSpace(config.Value.TableUrl, nameof(config.Value.TableUrl));

            _tableUrl = config.Value.TableUrl;

            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
            _tokenCredential = credentialProvider.GetCredential(TokenCredentialTypes.Internal);
        }

        public AzureTableClientFactory(
            ITokenCredentialProvider credentialProvider)
        {
            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));

            _tableUrl = StorageEmulatorConnectionString;
            _tokenCredential = credentialProvider.GetCredential(TokenCredentialTypes.Internal);
        }

        public TableClient Create(string tableName)
        {
            EnsureArg.IsNotNullOrWhiteSpace(tableName, nameof(tableName));

            // Create client for local emulator.
            if (string.Equals(_tableUrl, StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return new TableClient(_tableUrl, tableName);
            }

            var tableUri = new Uri(_tableUrl);

            return new TableClient(
                tableUri,
                tableName,
                _tokenCredential);
        }
    }
}