// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models.AzureStorage;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public interface IMetadataStore
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
        /// Attempts to update entity to azure table
        /// </summary>
        /// <param name="tableEntity">the table entity to update.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>return true if update entity successfully, return false if etag is not satisfied.</returns>
        public Task<bool> TryUpdateEntityAsync(ITableEntity tableEntity, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get trigger lease entity from azure table
        /// </summary>
        /// <param name="queueType">queue type.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Trigger lease entity, return null if does not exist.</returns>
        public Task<TriggerLeaseEntity> GetTriggerLeaseEntityAsync(byte queueType, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get current trigger entity from azure table
        /// </summary>
        /// <param name="queueType">queue type.</param>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>Current trigger entity, return null if does not exist.</returns>
        public Task<CurrentTriggerEntity> GetCurrentTriggerEntityAsync(byte queueType, CancellationToken cancellationToken = default);

        public Task<CompartmentInfoEntity> GetCompartmentInfoEntityAsync(byte queueType, string patientId, CancellationToken cancellationToken = default);

        public Task<Dictionary<string, long>> GetPatientVersionsAsync(
            byte queueType,
            List<string> patientsHash,
            CancellationToken cancellationToken = default);

        public Task<Dictionary<string, long>> GetPatientVersionsAsync(
            byte queueType,
            CancellationToken cancellationToken = default);

        public Task UpdatePatientVersionsAsync(byte queueType, Dictionary<string, long> patientVersions,  CancellationToken cancellationToken = default);

        public Task DeleteMetadataTableAsync();
    }
}