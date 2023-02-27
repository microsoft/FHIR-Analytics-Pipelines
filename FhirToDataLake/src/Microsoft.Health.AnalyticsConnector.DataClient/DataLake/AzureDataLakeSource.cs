// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.DataClient.Exceptions;

namespace Microsoft.Health.AnalyticsConnector.DataClient.DataLake
{
    public class AzureDataLakeSource : IDataLakeSource
    {
        public AzureDataLakeSource(IOptions<DataSourceConfiguration> configuration)
        {
            EnsureArg.IsNotNull(configuration, nameof(configuration));

            var dataSourceType = configuration.Value.Type;

            switch (dataSourceType)
            {
                case DataSourceType.FhirDataLakeStore:
                    StorageUrl = EnsureArg.IsNotNullOrEmpty(configuration.Value.FhirDataLakeStore.StorageUrl, nameof(configuration.Value.FhirDataLakeStore.StorageUrl));
                    Location = EnsureArg.IsNotNullOrEmpty(configuration.Value.FhirDataLakeStore.ContainerName, nameof(configuration.Value.FhirDataLakeStore.ContainerName));
                    break;
                default:
                    // Should not be thrown.
                    throw new DataLakeSearchException($"Unsupported data source type: {dataSourceType}");
            }
        }

        public string StorageUrl { get; }

        public string Location { get; }
    }
}
