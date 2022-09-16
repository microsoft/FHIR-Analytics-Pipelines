// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
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
        private readonly ITokenCredentialProvider _credentialProvider;
        private readonly string _tableUrl;
        private readonly string _internalConnectionString;

        public AzureTableClientFactory(
            IOptions<StorageConfiguration> storageConfiguration,
            IOptions<JobConfiguration> jobConfiguration,
            ITokenCredentialProvider credentialProvider)
        {
            EnsureArg.IsNotNull(storageConfiguration, nameof(storageConfiguration));
            _tableUrl = EnsureArg.IsNotNullOrWhiteSpace(jobConfiguration?.Value?.TableUrl, nameof(jobConfiguration.Value.TableUrl));
            _credentialProvider = EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));

            _internalConnectionString = storageConfiguration.Value.InternalStorageConnectionString;
        }

        public AzureTableClientFactory(
            ITokenCredentialProvider credentialProvider)
        {
            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));

            _tableUrl = StorageEmulatorConnectionString;
            _credentialProvider = EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
        }

        public TableClient Create(string tableName)
        {
            EnsureArg.IsNotNullOrWhiteSpace(tableName, nameof(tableName));

            // Create client for local emulator.
            if (string.Equals(_tableUrl, StorageEmulatorConnectionString, StringComparison.OrdinalIgnoreCase))
            {
                return new TableClient(_tableUrl, tableName);
            }

            if (!string.IsNullOrEmpty(_internalConnectionString))
            {
                return new TableClient(
                _internalConnectionString,
                tableName);
            }

            var tableUri = new Uri(_tableUrl);
            var tokenCredential = _credentialProvider.GetCredential(TokenCredentialTypes.Internal);

            return new TableClient(
                tableUri,
                tableName,
                tokenCredential);
        }
    }
}