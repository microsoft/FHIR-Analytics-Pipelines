// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Azure.Data.Tables;
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
        /// <typeparam name="TJobInfo">The job info type</typeparam>
        /// <param name="jobInfo">The job info.</param>
        /// <returns>Table entity of job info</returns>
        public static TableEntity ToTableEntity<TJobInfo>(this TJobInfo jobInfo)
            where TJobInfo : AzureStorageJobInfo, new()
        {
            var partitionKey = AzureStorageKeyProvider.JobInfoPartitionKey(jobInfo.QueueType, jobInfo.GroupId);
            var rowKey = AzureStorageKeyProvider.JobInfoRowKey(jobInfo.GroupId, jobInfo.Id);
            var jobInfoEntity = new TableEntity(partitionKey, rowKey)
            {
                { JobInfoEntityProperties.Id, jobInfo.Id },
                { JobInfoEntityProperties.QueueType, (int)jobInfo.QueueType },
                { JobInfoEntityProperties.Status, (int)(jobInfo.Status ?? JobStatus.Created) },
                { JobInfoEntityProperties.GroupId, jobInfo.GroupId },
                { JobInfoEntityProperties.Definition, jobInfo.Definition },
                { JobInfoEntityProperties.Result, jobInfo.Result },
                { JobInfoEntityProperties.Data, jobInfo.Data },
                { JobInfoEntityProperties.CancelRequested, jobInfo.CancelRequested },
                { JobInfoEntityProperties.Version, jobInfo.Version },
                { JobInfoEntityProperties.Priority, jobInfo.Priority },
                { JobInfoEntityProperties.CreateDate, (DateTimeOffset)jobInfo.CreateDate.SetKind(DateTimeKind.Utc) },
                { JobInfoEntityProperties.StartDate, (DateTimeOffset?)jobInfo.StartDate?.SetKind(DateTimeKind.Utc) },
                { JobInfoEntityProperties.EndDate, (DateTimeOffset?)jobInfo.EndDate?.SetKind(DateTimeKind.Utc) },
                { JobInfoEntityProperties.HeartbeatDateTime, (DateTimeOffset)jobInfo.HeartbeatDateTime.SetKind(DateTimeKind.Utc) },
                { JobInfoEntityProperties.HeartbeatTimeoutSec, jobInfo.HeartbeatTimeoutSec },
            };

            return jobInfoEntity;
        }

        /// <summary>
        /// Convert table entity to job info
        /// </summary>
        /// <typeparam name="TJobInfo">The job info type</typeparam>
        /// <param name="entity">The table entity</param>
        /// <returns>JobInfo</returns>
        public static TJobInfo ToJobInfo<TJobInfo>(this TableEntity entity)
            where TJobInfo : AzureStorageJobInfo, new()
        {
            var jobInfo = new TJobInfo
            {
                Id = (long)entity[JobInfoEntityProperties.Id],
                QueueType = Convert.ToByte(entity[JobInfoEntityProperties.QueueType]),
                Status = (JobStatus)Convert.ToByte(entity[JobInfoEntityProperties.Status]),
                GroupId = (long)entity[JobInfoEntityProperties.GroupId],
                Definition = (string)entity[JobInfoEntityProperties.Definition],
                Result = (string)entity[JobInfoEntityProperties.Result],
                Data = (long?)entity[JobInfoEntityProperties.Data],
                CancelRequested = (bool)entity[JobInfoEntityProperties.CancelRequested],
                Version = (long)entity[JobInfoEntityProperties.Version],
                Priority = (long)entity[JobInfoEntityProperties.Priority],
                CreateDate = ((DateTimeOffset)entity[JobInfoEntityProperties.CreateDate]).DateTime,
                StartDate = ((DateTimeOffset?)entity[JobInfoEntityProperties.StartDate])?.DateTime,
                EndDate = ((DateTimeOffset?)entity[JobInfoEntityProperties.EndDate])?.DateTime,
                HeartbeatDateTime = ((DateTimeOffset)entity[JobInfoEntityProperties.HeartbeatDateTime]).DateTime,
                HeartbeatTimeoutSec = (long)entity[JobInfoEntityProperties.HeartbeatTimeoutSec],
            };

            return jobInfo;
        }
    }
}