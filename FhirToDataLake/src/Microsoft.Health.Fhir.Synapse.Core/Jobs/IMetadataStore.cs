// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public interface IMetadataStore
    {
        public bool IsInitialized();

        public Task<Response> AddEntityAsync(ITableEntity tableEntity, CancellationToken cancellationToken = default);

        public Task<Response> UpdateEntityAsync(ITableEntity tableEntity, CancellationToken cancellationToken = default);

        public Task<TriggerLeaseEntity> GetTriggerLeaseEntityAsync(byte queueType, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get current trigger entity from azure table
        /// </summary>
        /// <param name="queueType">queue type.</param>
        /// <param name="cancellationToken">cancellation token</param>
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