// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models.AzureStorage;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class AzureTableMetadataStoreTests
    {
        private readonly NullLogger<AzureTableMetadataStore> _nullAzureTableMetadataStoreLogger =
            NullLogger<AzureTableMetadataStore>.Instance;

        private const byte QueueTypeByte = (byte) QueueType.FhirToDataLake;

        [Fact]
        public async Task GivenEmptyTable_WhenGetTriggerLeaseEntity_ThenFalseWillBeReturned()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                TriggerLeaseEntity entity = await metadataStore.GetTriggerLeaseEntityAsync(QueueTypeByte, CancellationToken.None);
                Assert.Null(entity);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenEmptyTable_WhenCurrentTriggerEntity_ThenNullWillBeReturned()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                CurrentTriggerEntity entity = await metadataStore.GetCurrentTriggerEntityAsync(QueueTypeByte, CancellationToken.None);
                Assert.Null(entity);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenCurrentTriggerEntity_WhenGetCurrentTriggerEntity_ThenTheEntityShouldBeReturned()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                CurrentTriggerEntity entity = new CurrentTriggerEntity
                {
                    PartitionKey = TableKeyProvider.TriggerPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.TriggerPartitionKey(QueueTypeByte),
                    TriggerStartTime = DateTimeOffset.UtcNow,
                    TriggerEndTime = DateTimeOffset.UtcNow,
                    TriggerStatus = TriggerStatus.New,
                    TriggerSequenceId = 0,
                };

                bool isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);

                CurrentTriggerEntity retrievedEntity = await metadataStore.GetCurrentTriggerEntityAsync(QueueTypeByte, CancellationToken.None);
                Assert.NotNull(retrievedEntity);
                Assert.Equal(entity.PartitionKey, retrievedEntity.PartitionKey);
                Assert.Equal(entity.RowKey, retrievedEntity.RowKey);
                Assert.Equal(entity.TriggerStatus, retrievedEntity.TriggerStatus);
                Assert.Equal(entity.TriggerSequenceId, retrievedEntity.TriggerSequenceId);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenEmptyTable_WhenCompartmentInfoEntity_ThenTheExceptionShouldBeThrown()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                RequestFailedException exception = await Assert.ThrowsAsync<RequestFailedException>(async () => await metadataStore.GetCompartmentInfoEntityAsync(QueueTypeByte, "fakepatientid", CancellationToken.None));
                Assert.Equal("ResourceNotFound", exception.ErrorCode);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenCompartmentInfoEntity_WhenGetCompartmentInfoEntity_ThenTheEntityShouldBeReturned()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                string patientId = "fakepatientId";
                CompartmentInfoEntity entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId),
                    VersionId = 1,
                };

                bool isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);

                CompartmentInfoEntity retrievedEntity = await metadataStore.GetCompartmentInfoEntityAsync(QueueTypeByte, patientId, CancellationToken.None);
                Assert.NotNull(retrievedEntity);
                Assert.Equal(entity.PartitionKey, retrievedEntity.PartitionKey);
                Assert.Equal(entity.RowKey, retrievedEntity.RowKey);
                Assert.Equal(entity.VersionId, retrievedEntity.VersionId);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenEmptyTable_WhenAddEntity_ThenTheEntityShouldBeAdded()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                Guid instanceGuid = Guid.NewGuid();
                TriggerLeaseEntity entity = new TriggerLeaseEntity
                {
                    PartitionKey = TableKeyProvider.LeasePartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.LeaseRowKey(QueueTypeByte),
                    WorkingInstanceGuid = instanceGuid,
                    HeartbeatDateTime = DateTimeOffset.UtcNow,
                };

                bool isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);
                TriggerLeaseEntity retrievedEntity =
                    await metadataStore.GetTriggerLeaseEntityAsync(QueueTypeByte, CancellationToken.None);
                Assert.NotNull(retrievedEntity);
                Assert.Equal(entity.PartitionKey, retrievedEntity.PartitionKey);
                Assert.Equal(entity.RowKey, retrievedEntity.RowKey);
                Assert.Equal(instanceGuid, retrievedEntity.WorkingInstanceGuid);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenExistingEntity_WhenAddSameEntityAgain_ThenFalseShouldBeReturned()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                Guid instanceGuid = Guid.NewGuid();
                TriggerLeaseEntity entity = new TriggerLeaseEntity
                {
                    PartitionKey = TableKeyProvider.LeasePartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.LeaseRowKey(QueueTypeByte),
                    WorkingInstanceGuid = instanceGuid,
                    HeartbeatDateTime = DateTimeOffset.UtcNow,
                };

                bool isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);

                Guid instanceGuid2 = Guid.NewGuid();
                TriggerLeaseEntity entity2 = new TriggerLeaseEntity
                {
                    PartitionKey = TableKeyProvider.LeasePartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.LeaseRowKey(QueueTypeByte),
                    WorkingInstanceGuid = instanceGuid2,
                    HeartbeatDateTime = DateTimeOffset.UtcNow,
                };

                isSucceeded = await metadataStore.TryAddEntityAsync(entity2);
                Assert.False(isSucceeded);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenExistingEntity_WhenUpdateEntity_ThenTheEntityShouldBeUpdated()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                Guid instanceGuid = Guid.NewGuid();
                TriggerLeaseEntity entity = new TriggerLeaseEntity
                {
                    PartitionKey = TableKeyProvider.LeasePartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.LeaseRowKey(QueueTypeByte),
                    WorkingInstanceGuid = instanceGuid,
                    HeartbeatDateTime = DateTimeOffset.UtcNow,
                };

                bool isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);
                TriggerLeaseEntity retrievedEntity =
                    await metadataStore.GetTriggerLeaseEntityAsync(QueueTypeByte, CancellationToken.None);
                Assert.NotNull(retrievedEntity);
                Assert.Equal(instanceGuid, retrievedEntity.WorkingInstanceGuid);
                Guid instanceGuid2 = Guid.NewGuid();
                retrievedEntity.WorkingInstanceGuid = instanceGuid2;
                retrievedEntity.HeartbeatDateTime = DateTimeOffset.UtcNow;
                isSucceeded = await metadataStore.TryUpdateEntityAsync(retrievedEntity);
                Assert.True(isSucceeded);
                TriggerLeaseEntity retrievedEntity2 =
                    await metadataStore.GetTriggerLeaseEntityAsync(QueueTypeByte, CancellationToken.None);
                Assert.NotNull(retrievedEntity2);
                Assert.Equal(instanceGuid2, retrievedEntity2.WorkingInstanceGuid);
                Assert.True(retrievedEntity2.Timestamp > retrievedEntity.Timestamp);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenOldEntity_WhenUpdateEntity_ThenFalseShouldBeReturned()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                Guid instanceGuid = Guid.NewGuid();
                TriggerLeaseEntity entity = new TriggerLeaseEntity
                {
                    PartitionKey = TableKeyProvider.LeasePartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.LeaseRowKey(QueueTypeByte),
                    WorkingInstanceGuid = instanceGuid,
                    HeartbeatDateTime = DateTimeOffset.UtcNow,
                };

                bool isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);
                TriggerLeaseEntity retrievedEntity =
                    await metadataStore.GetTriggerLeaseEntityAsync(QueueTypeByte, CancellationToken.None);
                Assert.NotNull(retrievedEntity);
                Assert.Equal(instanceGuid, retrievedEntity.WorkingInstanceGuid);
                Guid instanceGuid2 = Guid.NewGuid();
                retrievedEntity.WorkingInstanceGuid = instanceGuid2;
                retrievedEntity.HeartbeatDateTime = DateTimeOffset.UtcNow;
                isSucceeded = await metadataStore.TryUpdateEntityAsync(retrievedEntity);
                Assert.True(isSucceeded);
                TriggerLeaseEntity retrievedEntity2 =
                    await metadataStore.GetTriggerLeaseEntityAsync(QueueTypeByte, CancellationToken.None);
                Assert.NotNull(retrievedEntity2);
                Assert.Equal(instanceGuid2, retrievedEntity2.WorkingInstanceGuid);
                Assert.True(retrievedEntity2.Timestamp > retrievedEntity.Timestamp);

                // fail to update entity with old etag
                retrievedEntity.WorkingInstanceGuid = instanceGuid;
                retrievedEntity.HeartbeatDateTime = DateTimeOffset.UtcNow;
                isSucceeded = await metadataStore.TryUpdateEntityAsync(retrievedEntity);
                Assert.False(isSucceeded);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenCompartmentInfoEntities_WhenGetPatientVersionsByPatientHashList_ThenTheResultShouldBeReturned()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                string patientId1 = "patientId1";
                CompartmentInfoEntity entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId1),
                    VersionId = 1,
                };

                bool isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);

                string patientId2 = "patientId2";
                entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId2),
                    VersionId = 1,
                };

                isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);

                List<string> patientHashList = new List<string>
                    { TableKeyProvider.CompartmentRowKey(patientId1) };
                Dictionary<string, long> patientVersions = await metadataStore.GetPatientVersionsAsync(QueueTypeByte, patientHashList, CancellationToken.None);
                Assert.Single(patientVersions);
                Assert.True(patientVersions.ContainsKey(TableKeyProvider.CompartmentRowKey(patientId1)));
                Assert.Equal(1, patientVersions[TableKeyProvider.CompartmentRowKey(patientId1)]);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenCompartmentInfoEntities_WhenGetPatientVersions_ThenTheResultShouldBeReturned()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                string patientId1 = "patientId1";
                CompartmentInfoEntity entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId1),
                    VersionId = 1,
                };

                bool isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);

                string patientId2 = "patientId2";
                entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId2),
                    VersionId = 1,
                };

                isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);

                Dictionary<string, long> patientVersions = await metadataStore.GetPatientVersionsAsync(QueueTypeByte,  CancellationToken.None);
                Assert.Equal(2, patientVersions.Count);
                Assert.True(patientVersions.ContainsKey(TableKeyProvider.CompartmentRowKey(patientId1)));
                Assert.True(patientVersions.ContainsKey(TableKeyProvider.CompartmentRowKey(patientId1)));
                Assert.Equal(1, patientVersions[TableKeyProvider.CompartmentRowKey(patientId1)]);
                Assert.Equal(1, patientVersions[TableKeyProvider.CompartmentRowKey(patientId2)]);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenCompartmentInfoEntities_WhenUpdateGetPatientVersions_ThenTheEntitiesShouldBeUpdated()
        {
            IMetadataStore metadataStore = CreateUniqueMetadataStore();
            try
            {
                string patientId1 = "patientId1";
                CompartmentInfoEntity entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId1),
                    VersionId = 1,
                };

                bool isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);

                string patientId2 = "patientId2";
                entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId2),
                    VersionId = 1,
                };

                isSucceeded = await metadataStore.TryAddEntityAsync(entity);
                Assert.True(isSucceeded);

                Dictionary<string, long> updatedPatientVersions = new Dictionary<string, long> { { TableKeyProvider.CompartmentRowKey(patientId1), 2 } };
                await metadataStore.UpdatePatientVersionsAsync(QueueTypeByte, updatedPatientVersions);

                Dictionary<string, long> patientVersions = await metadataStore.GetPatientVersionsAsync(QueueTypeByte, CancellationToken.None);
                Assert.Equal(2, patientVersions.Count);
                Assert.True(patientVersions.ContainsKey(TableKeyProvider.CompartmentRowKey(patientId1)));
                Assert.True(patientVersions.ContainsKey(TableKeyProvider.CompartmentRowKey(patientId1)));
                Assert.Equal(2, patientVersions[TableKeyProvider.CompartmentRowKey(patientId1)]);
                Assert.Equal(1, patientVersions[TableKeyProvider.CompartmentRowKey(patientId2)]);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        private IMetadataStore CreateUniqueMetadataStore()
        {
            string uniqueName = Guid.NewGuid().ToString("N");
            IOptions<JobConfiguration> jobConfig = Options.Create(new JobConfiguration
            {
                JobInfoTableName = $"jobinfotable{uniqueName}",
                MetadataTableName = $"metadatatable{uniqueName}",
                JobInfoQueueName = $"jobinfoqueue{uniqueName}",
            });

            // Make sure the container is deleted before running the tests
            AzureTableClientFactory azureTableClientFactory = new AzureTableClientFactory(
                new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()));

            AzureTableMetadataStore metadataStore = new AzureTableMetadataStore(azureTableClientFactory, jobConfig, _nullAzureTableMetadataStoreLogger);
            Assert.True(metadataStore.IsInitialized());
            return metadataStore;
        }
    }
}
