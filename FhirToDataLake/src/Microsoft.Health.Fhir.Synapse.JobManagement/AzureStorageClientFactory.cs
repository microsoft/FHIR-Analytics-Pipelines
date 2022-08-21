// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public class AzureStorageClientFactory : IAzureStorageClientFactory
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ITokenCredentialProvider _credentialProvider;

        public AzureStorageClientFactory(
            ITokenCredentialProvider credentialProvider,
            ILoggerFactory loggerFactory)
        {
            EnsureArg.IsNotNull(credentialProvider, nameof(credentialProvider));
            EnsureArg.IsNotNull(loggerFactory, nameof(loggerFactory));

            _credentialProvider = credentialProvider;
            _loggerFactory = loggerFactory;
        }

        public IAzureStorageClient Create(IStorage storage)
        {
            EnsureArg.IsNotNull(storage, nameof(storage));

            // Create client for local emulator.
            // TODO: ConnectionString is only used for local test
            if (storage.UseConnectionString)
            {
                return new AzureStorageClient(storage.TableUrl, storage.TableName, storage.QueueUrl, storage.QueueName, _loggerFactory.CreateLogger<AzureStorageClient>());
            }

            var tableUri = new Uri(storage.TableUrl);
            var queueUri = new Uri($"{storage.QueueUrl}{storage.QueueName}");
            return new AzureStorageClient(
                tableUri,
                storage.TableName,
                queueUri,
                _credentialProvider.GetCredential(TokenCredentialTypes.Internal),
                _loggerFactory.CreateLogger<AzureStorageClient>());
        }
    }
}