// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Microsoft.Health.AnalyticsConnector.DataWriter.Azure;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public interface ICompletedBlobStore
    {
        public bool IsInitialized();

        /// <summary>
        /// Attempts to add entity to azure table
        /// </summary>
        /// <param name="tableEntity">the table entity to add.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>return true if add entity successfully, return false if the entity already exists.</returns>
        public Task<bool> TryAddEntityAsync(ITableEntity tableEntity, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get AzureBlobInfo for completed blobs
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>list of AzureBlobInfo.</returns>
        public Task<List<AzureBlobInfo>> GetCompletedBlobsAsync(
            CancellationToken cancellationToken = default);

        public Task DeleteCompletedBlobTableAsync();
    }
}
