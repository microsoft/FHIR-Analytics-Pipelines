// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

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
        public void GivenNullJobInfo_WhenToTableEntity_ExceptionShouldBeThrown()
        {
            AzureStorageJobInfo? jobInfo = null;
            Assert.Throws<NullReferenceException>(() => jobInfo.ToTableEntity());
        }

        [Fact]
        public void GivenDefaultJobInfo_WhenToTableEntity_CorrectTableEntityShouldBeReturned()
        {
            var jobInfo = new AzureStorageJobInfo();
            var tableEntity = jobInfo.ToTableEntity();
            Assert.NotNull(tableEntity);
            Assert.Null(jobInfo.Status);
            Assert.Equal((int)JobStatus.Created, tableEntity.Status);
            Assert.Null(tableEntity.Definition);
            Assert.Null(tableEntity.Result);
        }

        [Fact]
        public void GivenAzureStorageJobInfo_WhenToTableEntity_CorrectTableEntityShouldBeReturned()
        {
            var jobInfo = new AzureStorageJobInfo();
            var tableEntity = jobInfo.ToTableEntity();
            Assert.NotNull(tableEntity);
            Assert.Null(jobInfo.Status);
            Assert.Equal((int)JobStatus.Created, tableEntity.Status);
            Assert.Null(tableEntity.Definition);
            Assert.Null(tableEntity.Result);
        }

        [Fact]
        public void GivenValidJobInfo_WhenToTableEntity_CorrectTableEntityShouldBeReturned()
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
            Assert.Equal(jobInfo.Id, tableEntity.Id);
            Assert.Equal(jobInfo.QueueType, tableEntity.QueueType);
            Assert.Equal((int)jobInfo.Status, tableEntity.Status);
            Assert.Equal(jobInfo.GroupId, tableEntity.GroupId);
            Assert.Equal(jobInfo.Definition, tableEntity.Definition);
            Assert.Equal(jobInfo.Result, tableEntity.Result);
            Assert.Equal(jobInfo.CancelRequested, tableEntity.CancelRequested);
            Assert.Equal(jobInfo.CreateDate, tableEntity.CreateDate);
            Assert.Equal(jobInfo.HeartbeatDateTime, tableEntity.HeartbeatDateTime);
        }

        [Fact]
        public void GivenNullTableEntity_WhenToJobInfo_ExceptionShouldBeThrown()
        {
            JobInfoEntity? jobInfoEntity = null;
            Assert.Throws<NullReferenceException>(() => jobInfoEntity.ToJobInfo<FhirToDataLakeAzureStorageJobInfo>());
        }

        [Fact]
        public void GivenDefaultTableEntity_WhenToJobInfo_CorrectResultShouldBeReturned()
        {
            var jobInfoEntity = new JobInfoEntity
            {
                PartitionKey = "partitionKey",
                RowKey = "rowKey",
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };
            var jobInfo = jobInfoEntity.ToJobInfo<FhirToDataLakeAzureStorageJobInfo>();
            Assert.NotNull(jobInfo);
            Assert.Equal(JobStatus.Created, jobInfo.Status);
            Assert.Null(jobInfo.Definition);
            Assert.Null(jobInfo.Result);
        }

        [Fact]
        public void GivenValidTableEntity_WhenToJobInfo_CorrectResultShouldBeReturned()
        {
            var jobInfoEntity = new JobInfoEntity
            {
                PartitionKey = "partitionKey",
                RowKey = "rowKey",
                Id = 1,
                QueueType = (int)QueueType.FhirToDataLake,
                Status = (int)JobStatus.Created,
                GroupId = 0,
                Definition = "input data string",
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };
            var jobInfo = jobInfoEntity.ToJobInfo<FhirToDataLakeAzureStorageJobInfo>();
            Assert.NotNull(jobInfo);
            Assert.Equal(jobInfoEntity.Id, jobInfo.Id);
            Assert.Equal(jobInfoEntity.QueueType, jobInfo.QueueType);
            Assert.NotNull(jobInfo.Status);
            Assert.Equal(jobInfoEntity.Status, (int)jobInfo.Status);
            Assert.Equal(jobInfoEntity.GroupId, jobInfo.GroupId);
            Assert.Equal(jobInfoEntity.Definition, jobInfo.Definition);
            Assert.Equal(jobInfoEntity.Result, jobInfo.Result);
            Assert.Equal(jobInfoEntity.CancelRequested, jobInfo.CancelRequested);
            Assert.Equal(jobInfoEntity.CreateDate, jobInfo.CreateDate);
            Assert.Equal(jobInfoEntity.HeartbeatDateTime, jobInfo.HeartbeatDateTime);
        }
    }
}