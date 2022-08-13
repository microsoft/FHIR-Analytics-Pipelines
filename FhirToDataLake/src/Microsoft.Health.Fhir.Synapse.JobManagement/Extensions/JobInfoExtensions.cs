// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models.AzureStorage;
using Microsoft.Health.JobManagement;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.Extensions
{
    public static class JobInfoExtensions
    {
        /// <summary>
        /// Convert job info to table entity
        /// </summary>
        /// <param name="jobInfo">the job info.</param>
        /// <returns>JobInfoEntity</returns>
        public static JobInfoEntity ToTableEntity(this JobInfo jobInfo)
        {
            var jobInfoEntity = new JobInfoEntity
            {
                PartitionKey = AzureStorageKeyProvider.JobInfoPartitionKey(jobInfo.QueueType, jobInfo.GroupId),
                RowKey = AzureStorageKeyProvider.JobInfoRowKey(jobInfo.GroupId, jobInfo.Id),
                Id = jobInfo.Id,
                QueueType = jobInfo.QueueType,
                Status = (int)(jobInfo.Status ?? JobStatus.Created),
                GroupId = jobInfo.GroupId,
                Definition = jobInfo.Definition,
                Result = jobInfo.Result,
                Data = jobInfo.Data,
                CancelRequested = jobInfo.CancelRequested,
                Version = jobInfo.Version,
                Priority = jobInfo.Priority,
                CreateDate = jobInfo.CreateDate.SetKind(DateTimeKind.Utc),
                StartDate = jobInfo.StartDate?.SetKind(DateTimeKind.Utc),
                EndDate = jobInfo.EndDate?.SetKind(DateTimeKind.Utc),
                HeartbeatDateTime = jobInfo.HeartbeatDateTime.SetKind(DateTimeKind.Utc),
            };

            return jobInfoEntity;
        }

        /// <summary>
        /// Convert table entity to job info
        /// </summary>
        /// <typeparam name="TJobInfo">the job info type</typeparam>
        /// <param name="entity">the table entity</param>
        /// <returns>JobInfo</returns>
        public static TJobInfo ToJobInfo<TJobInfo>(this JobInfoEntity entity)
            where TJobInfo : AzureStorageJobInfo, new()
        {
            var jobInfo = new TJobInfo
            {
                Id = entity.Id,
                QueueType = Convert.ToByte(entity.QueueType),
                Status = (JobStatus)Convert.ToByte(entity.Status),
                GroupId = entity.GroupId,
                Definition = entity.Definition,
                Result = entity.Result,
                Data = entity.Data,
                CancelRequested = entity.CancelRequested,
                Version = entity.Version,
                Priority = entity.Priority,
                CreateDate = ((DateTimeOffset)entity.CreateDate).DateTime,
                StartDate = ((DateTimeOffset?)entity.StartDate)?.DateTime,
                EndDate = ((DateTimeOffset?)entity.EndDate)?.DateTime,
                HeartbeatDateTime = ((DateTimeOffset)entity.HeartbeatDateTime).DateTime,
            };

            return jobInfo;
        }

        public static DateTime SetKind(this DateTime dateTime, DateTimeKind dateTimeKind)
        {
            return DateTime.SpecifyKind(dateTime, dateTimeKind);
        }
    }
}