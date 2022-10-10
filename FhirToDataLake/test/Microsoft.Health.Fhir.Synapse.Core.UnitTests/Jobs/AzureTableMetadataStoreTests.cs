// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
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
using Newtonsoft.Json;
using Xunit;
using JobStatus = Microsoft.Health.JobManagement.JobStatus;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class AzureTableMetadataStoreTests
    {
        private readonly NullLogger<AzureTableMetadataStore> _nullAzureTableMetadataStoreLogger =
            NullLogger<AzureTableMetadataStore>.Instance;

        private const byte QueueTypeByte = (byte) QueueType.FhirToDataLake;

        [Fact]
        public async Task GivenEmptyTable_WhenGetTriggerLeaseEntity_ThenTheExceptionShouldBeThrown()
        {
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var exception = await Assert.ThrowsAsync<RequestFailedException>(async () => await metadataStore.GetTriggerLeaseEntityAsync(QueueTypeByte, CancellationToken.None));
                Assert.Equal("ResourceNotFound", exception.ErrorCode);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenEmptyTable_WhenCurrentTriggerEntity_ThenNullWillBeReturned()
        {
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var entity = await metadataStore.GetCurrentTriggerEntityAsync(QueueTypeByte, CancellationToken.None);
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
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var entity = new CurrentTriggerEntity
                {
                    PartitionKey = TableKeyProvider.TriggerPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.TriggerPartitionKey(QueueTypeByte),
                    TriggerStartTime = DateTimeOffset.UtcNow,
                    TriggerEndTime = DateTimeOffset.UtcNow,
                    TriggerStatus = TriggerStatus.New,
                    TriggerSequenceId = 0,
                };

                var response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);

                var retrievedEntity = await metadataStore.GetCurrentTriggerEntityAsync(QueueTypeByte, CancellationToken.None);
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
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var exception = await Assert.ThrowsAsync<RequestFailedException>(async () => await metadataStore.GetCompartmentInfoEntityAsync(QueueTypeByte, "fakepatientid", CancellationToken.None));
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
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var patientId = "fakepatientId";
                var entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId),
                    VersionId = 1,
                };

                var response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);

                var retrievedEntity = await metadataStore.GetCompartmentInfoEntityAsync(QueueTypeByte, patientId, CancellationToken.None);
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
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var instanceGuid = Guid.NewGuid();
                var entity = new TriggerLeaseEntity
                {
                    PartitionKey = TableKeyProvider.LeasePartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.LeaseRowKey(QueueTypeByte),
                    WorkingInstanceGuid = instanceGuid,
                    HeartbeatDateTime = DateTimeOffset.UtcNow,
                };

                var response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);
                var retrievedEntity =
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
        public async Task GivenExistingEntity_WhenAddSameEntityAgain_ThenTheExceptionShouldBeThrown()
        {
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var instanceGuid = Guid.NewGuid();
                var entity = new TriggerLeaseEntity
                {
                    PartitionKey = TableKeyProvider.LeasePartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.LeaseRowKey(QueueTypeByte),
                    WorkingInstanceGuid = instanceGuid,
                    HeartbeatDateTime = DateTimeOffset.UtcNow,
                };

                var response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);

                var instanceGuid2 = Guid.NewGuid();
                var entity2 = new TriggerLeaseEntity
                {
                    PartitionKey = TableKeyProvider.LeasePartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.LeaseRowKey(QueueTypeByte),
                    WorkingInstanceGuid = instanceGuid2,
                    HeartbeatDateTime = DateTimeOffset.UtcNow,
                };
                var exception = await Assert.ThrowsAsync<RequestFailedException>(async () => await metadataStore.AddEntityAsync(entity2));
                Assert.Equal("EntityAlreadyExists", exception.ErrorCode);
            }
            finally
            {
                await metadataStore.DeleteMetadataTableAsync();
            }
        }

        [Fact]
        public async Task GivenExistingEntity_WhenUpdateEntity_ThenTheEntityShouldBeUpdated()
        {
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var instanceGuid = Guid.NewGuid();
                var entity = new TriggerLeaseEntity
                {
                    PartitionKey = TableKeyProvider.LeasePartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.LeaseRowKey(QueueTypeByte),
                    WorkingInstanceGuid = instanceGuid,
                    HeartbeatDateTime = DateTimeOffset.UtcNow,
                };

                var response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);
                var retrievedEntity =
                    await metadataStore.GetTriggerLeaseEntityAsync(QueueTypeByte, CancellationToken.None);
                Assert.NotNull(retrievedEntity);
                Assert.Equal(instanceGuid, retrievedEntity.WorkingInstanceGuid);
                var instanceGuid2 = Guid.NewGuid();
                retrievedEntity.WorkingInstanceGuid = instanceGuid2;
                retrievedEntity.HeartbeatDateTime = DateTimeOffset.UtcNow;
                response = await metadataStore.UpdateEntityAsync(retrievedEntity);
                Assert.Equal(204, response.Status);
                var retrievedEntity2 =
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
        public async Task GivenCompartmentInfoEntities_WhenGetPatientVersionsByPatientHashList_ThenTheResultShouldBeReturned()
        {
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var patientId1 = "patientId1";
                var entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId1),
                    VersionId = 1,
                };

                var response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);

                var patientId2 = "patientId2";
                entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId2),
                    VersionId = 1,
                };

                response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);

                var patientHashList = new List<string>
                    { TableKeyProvider.CompartmentRowKey(patientId1) };
                var patientVersions = await metadataStore.GetPatientVersionsAsync(QueueTypeByte, patientHashList, CancellationToken.None);
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
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var patientId1 = "patientId1";
                var entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId1),
                    VersionId = 1,
                };

                var response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);

                var patientId2 = "patientId2";
                entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId2),
                    VersionId = 1,
                };

                response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);

                var patientVersions = await metadataStore.GetPatientVersionsAsync(QueueTypeByte,  CancellationToken.None);
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
            var metadataStore = CreateUniqueMetadataStore();
            try
            {
                var patientId1 = "patientId1";
                var entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId1),
                    VersionId = 1,
                };

                var response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);

                var patientId2 = "patientId2";
                entity = new CompartmentInfoEntity
                {
                    PartitionKey = TableKeyProvider.CompartmentPartitionKey(QueueTypeByte),
                    RowKey = TableKeyProvider.CompartmentRowKey(patientId2),
                    VersionId = 1,
                };

                response = await metadataStore.AddEntityAsync(entity);
                Assert.Equal(204, response.Status);

                var updatedPatientVersions = new Dictionary<string, long> { { TableKeyProvider.CompartmentRowKey(patientId1), 2 } };
                await metadataStore.UpdatePatientVersionsAsync(QueueTypeByte, updatedPatientVersions);

                var patientVersions = await metadataStore.GetPatientVersionsAsync(QueueTypeByte, CancellationToken.None);
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
            var uniqueName = Guid.NewGuid().ToString("N");
            var jobConfig = Options.Create(new JobConfiguration
            {
                JobInfoTableName = $"jobinfotable{uniqueName}",
                MetadataTableName = $"metadatatable{uniqueName}",
                JobInfoQueueName = $"jobinfoqueue{uniqueName}",
            });

            // Make sure the container is deleted before running the tests
            var azureTableClientFactory = new AzureTableClientFactory(
                new DefaultTokenCredentialProvider(new NullLogger<DefaultTokenCredentialProvider>()));

            var metadataStore = new AzureTableMetadataStore(azureTableClientFactory, jobConfig, _nullAzureTableMetadataStoreLogger);
            Assert.True(metadataStore.IsInitialized());
            return metadataStore;
        }
    }
}
