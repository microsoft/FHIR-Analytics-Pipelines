// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class MockMetadataStore : IMetadataStore
    {
        private Dictionary<Tuple<string, string>, ITableEntity> _entities = new Dictionary<Tuple<string, string>, ITableEntity>();

        public Dictionary<Tuple<string, string>, ITableEntity> Entities => _entities;

        public Func<MockMetadataStore, byte, CancellationToken, TriggerLeaseEntity> GetTriggerLeaseEntityFunc { get; set; }

        public bool Initialized { get; set; } = true;

        public bool IsInitialized()
        {
            return Initialized;
        }

        public Task<bool> TryAddEntityAsync(ITableEntity tableEntity, CancellationToken cancellationToken = default)
        {
            Tuple<string, string> key = new Tuple<string, string>(tableEntity.PartitionKey, tableEntity.RowKey);
            if (_entities.ContainsKey(key))
            {
                return Task.FromResult(false);
            }

            var entity = tableEntity.CloneObject() as ITableEntity;
            entity.ETag = new ETag(Guid.NewGuid().ToString());

            _entities[key] = entity;
            return Task.FromResult(true);
        }

        public Task<bool> TryUpdateEntityAsync(ITableEntity tableEntity, CancellationToken cancellationToken = default)
        {
            Tuple<string, string> key = new Tuple<string, string>(tableEntity.PartitionKey, tableEntity.RowKey);
            if (_entities.ContainsKey(key) && _entities[key].ETag != tableEntity.ETag)
            {
                return Task.FromResult(false);
            }

            var entity = tableEntity.CloneObject() as ITableEntity;
            entity.ETag = new ETag(Guid.NewGuid().ToString());

            _entities[key] = entity;
            return Task.FromResult(true);
        }

        public Task<TriggerLeaseEntity> GetTriggerLeaseEntityAsync(byte queueType, CancellationToken cancellationToken = default)
        {
            if (GetTriggerLeaseEntityFunc != null)
            {
                return Task.FromResult(GetTriggerLeaseEntityFunc(this, queueType, cancellationToken));
            }

            Tuple<string, string> key = TriggerLeaseEntityKey(queueType);
            return Task.FromResult(_entities.ContainsKey(key) ? (TriggerLeaseEntity)_entities[key] : null);
        }

        public Task<CurrentTriggerEntity> GetCurrentTriggerEntityAsync(byte queueType, CancellationToken cancellationToken = default)
        {
            Tuple<string, string> key = CurrentTriggerEntityKey(queueType);
            return Task.FromResult(_entities.ContainsKey(key) ? (CurrentTriggerEntity)_entities[key] : null);
        }

        public Task<CompartmentInfoEntity> GetCompartmentInfoEntityAsync(byte queueType, string patientId, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<Dictionary<string, long>> GetPatientVersionsAsync(byte queueType, List<string> patientsHash, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<Dictionary<string, long>> GetPatientVersionsAsync(byte queueType, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task UpdatePatientVersionsAsync(byte queueType, Dictionary<string, long> patientVersions, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task DeleteMetadataTableAsync()
        {
            throw new NotImplementedException();
        }

        private Tuple<string, string> CurrentTriggerEntityKey(byte queueType) =>
            new Tuple<string, string>(TableKeyProvider.TriggerPartitionKey(queueType), TableKeyProvider.TriggerRowKey(queueType));

        private Tuple<string, string> TriggerLeaseEntityKey(byte queueType) =>
            new Tuple<string, string>(TableKeyProvider.LeasePartitionKey(queueType), TableKeyProvider.LeaseRowKey(queueType));
    }
}
