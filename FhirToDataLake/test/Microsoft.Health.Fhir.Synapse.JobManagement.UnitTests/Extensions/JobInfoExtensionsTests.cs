// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Data.Tables;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage;
using Xunit;
using JobStatus = Microsoft.Health.JobManagement.JobStatus;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.UnitTests.Extensions
{
    public class JobInfoExtensionsTests
    {
        [Fact]
        public void GivenNullJobInfo_WhenToTableEntity_ThenTheExceptionShouldBeThrown()
        {
            AzureStorageJobInfo? jobInfo = null;
            Assert.Throws<NullReferenceException>(() => jobInfo.ToTableEntity());
        }

        [Fact]
        public void GivenDefaultJobInfo_WhenToTableEntity_ThenTheCorrectTableEntityShouldBeReturned()
        {
            var jobInfo = new AzureStorageJobInfo();
            var tableEntity = jobInfo.ToTableEntity();
            Assert.NotNull(tableEntity);
            Assert.Null(jobInfo.Status);
            Assert.Equal((int)JobStatus.Created, (int)tableEntity[JobInfoEntityProperties.Status]);
            Assert.Null(tableEntity[JobInfoEntityProperties.Definition]);
            Assert.Null(tableEntity[JobInfoEntityProperties.Result]);
        }

        [Fact]
        public void GivenAzureStorageJobInfo_WhenToTableEntity_ThenTheCorrectTableEntityShouldBeReturned()
        {
            var jobInfo = new AzureStorageJobInfo();
            var tableEntity = jobInfo.ToTableEntity();
            Assert.NotNull(tableEntity);
            Assert.Null(jobInfo.Status);
            Assert.Equal((int)JobStatus.Created, (int)tableEntity[JobInfoEntityProperties.Status]);
            Assert.Null(tableEntity[JobInfoEntityProperties.Definition]);
            Assert.Null(tableEntity[JobInfoEntityProperties.Result]);
        }

        [Fact]
        public void GivenValidJobInfo_WhenToTableEntity_ThenTheCorrectTableEntityShouldBeReturned()
        {
            var jobInfo = new FhirToDataLakeAzureStorageJobInfo
            {
                Id = 1,
                QueueType = (byte)QueueType.FhirToDataLake,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = "input data string",
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };
            var tableEntity = jobInfo.ToTableEntity();
            Assert.NotNull(tableEntity);
            Assert.Equal(jobInfo.Id, (long)tableEntity[JobInfoEntityProperties.Id]);
            Assert.Equal(jobInfo.QueueType, (int)tableEntity[JobInfoEntityProperties.QueueType]);
            Assert.Equal((int)jobInfo.Status, (int)tableEntity[JobInfoEntityProperties.Status]);
            Assert.Equal(jobInfo.GroupId, (long)tableEntity[JobInfoEntityProperties.GroupId]);
            Assert.Equal(jobInfo.Definition, tableEntity[JobInfoEntityProperties.Definition].ToString());
            Assert.Equal(jobInfo.Result, tableEntity[JobInfoEntityProperties.Result].ToString());
            Assert.Equal(jobInfo.CancelRequested, (bool)tableEntity[JobInfoEntityProperties.CancelRequested]);
            Assert.Equal(jobInfo.CreateDate, (DateTimeOffset)tableEntity[JobInfoEntityProperties.CreateDate]);
            Assert.Equal(jobInfo.HeartbeatDateTime, (DateTimeOffset)tableEntity[JobInfoEntityProperties.HeartbeatDateTime]);
        }

        [Fact]
        public void GivenNullTableEntity_WhenToJobInfo_ThenTheExceptionShouldBeThrown()
        {
            TableEntity? jobInfoEntity = null;
            Assert.Throws<NullReferenceException>(() => jobInfoEntity.ToJobInfo<FhirToDataLakeAzureStorageJobInfo>());
        }

        [Fact]
        public void GivenValidTableEntity_WhenToJobInfo_ThenTheCorrectResultShouldBeReturned()
        {
            var jobInfoEntity = new TableEntity("partitionKey", "rowKey")
            {
                { JobInfoEntityProperties.Id, (long)1 },
                { JobInfoEntityProperties.QueueType, (int)QueueType.FhirToDataLake },
                { JobInfoEntityProperties.Status, (int)JobStatus.Created },
                { JobInfoEntityProperties.GroupId, (long)0 },
                { JobInfoEntityProperties.Definition, "input data string" },
                { JobInfoEntityProperties.Result, string.Empty },
                { JobInfoEntityProperties.CancelRequested, false },
                { JobInfoEntityProperties.Version, (long)0 },
                { JobInfoEntityProperties.Priority, (long)0 },
                { JobInfoEntityProperties.CreateDate, DateTimeOffset.Now },
                { JobInfoEntityProperties.HeartbeatDateTime, DateTimeOffset.Now },
                { JobInfoEntityProperties.HeartbeatTimeoutSec, (long)0 },
            };
            var jobInfo = jobInfoEntity.ToJobInfo<FhirToDataLakeAzureStorageJobInfo>();
            Assert.NotNull(jobInfo);
            Assert.Equal((long)jobInfoEntity[JobInfoEntityProperties.Id], jobInfo.Id);
            Assert.Equal((int)jobInfoEntity[JobInfoEntityProperties.QueueType], jobInfo.QueueType);
            Assert.NotNull(jobInfo.Status);
            Assert.Equal((int)jobInfoEntity[JobInfoEntityProperties.Status], (int)jobInfo.Status);
            Assert.Equal((long)jobInfoEntity[JobInfoEntityProperties.GroupId], jobInfo.GroupId);
            Assert.Equal(jobInfoEntity[JobInfoEntityProperties.Definition].ToString(), jobInfo.Definition);
            Assert.Equal(jobInfoEntity[JobInfoEntityProperties.Result].ToString(), jobInfo.Result);
            Assert.Equal((bool)jobInfoEntity[JobInfoEntityProperties.CancelRequested], jobInfo.CancelRequested);
            Assert.Equal((DateTimeOffset)jobInfoEntity[JobInfoEntityProperties.CreateDate], jobInfo.CreateDate);
            Assert.Equal((DateTimeOffset)jobInfoEntity[JobInfoEntityProperties.HeartbeatDateTime], jobInfo.HeartbeatDateTime);
        }
    }
}