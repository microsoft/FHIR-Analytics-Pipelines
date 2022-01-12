// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;

namespace Microsoft.Health.Fhir.Synapse.DataWriter.Azure
{
    public class AzureBlobDataSink : IDataSink
    {
        public AzureBlobDataSink(
            IOptions<DataLakeStoreConfiguration> storageConfig,
            IOptions<JobConfiguration> jobConfig)
        {
            EnsureArg.IsNotNull(storageConfig, nameof(storageConfig));
            EnsureArg.IsNotNull(jobConfig, nameof(jobConfig));

            StorageUrl = storageConfig.Value.StorageUrl;
            Location = jobConfig.Value.ContainerName;
        }

        public string StorageUrl { get; }

        public string Location { get; }
    }
}
