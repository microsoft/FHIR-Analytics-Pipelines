// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure.Data.Tables;
using Microsoft.Health.JobManagement;

namespace AzureTableTaskQueue.Extensions
{
    public static class JobInfoExtensions
    {
        public static TableEntity ToTableEntity(this JobInfo jobInfo)
        {
            string partitionKey = $"{jobInfo.QueueType:D3}_{jobInfo.GroupId:D20}";
            string rowKey = $"{jobInfo.GroupId:D20}_{jobInfo.Id:D20}";
            var entity = new TableEntity(partitionKey, rowKey)
            {
                { "Id", jobInfo.Id },
                { "QueueType", (int)jobInfo.QueueType },
                { "Status", (int)(jobInfo.Status ?? JobStatus.Created) },
                { "GroupId", jobInfo.GroupId },
                { "Definition", jobInfo.Definition },
                { "Result", jobInfo.Result },
                { "Data", jobInfo.Data },
                { "CancelRequested", jobInfo.CancelRequested },
                { "Version", jobInfo.Version },
                { "Priority", jobInfo.Priority },
                { "CreateDate", jobInfo.CreateDate.SetKind(DateTimeKind.Utc) },
                { "StartDate", jobInfo.StartDate?.SetKind(DateTimeKind.Utc) },
                { "EndDate", jobInfo.EndDate?.SetKind(DateTimeKind.Utc) },
                { "HeartbeatDateTime", jobInfo.HeartbeatDateTime.SetKind(DateTimeKind.Utc) },
            };

            return entity;
        }

        public static JobInfo ToJobInfo(this TableEntity entity)
        {
            var jobInfo = new JobInfo();
            jobInfo.Id = (long)entity["Id"];
            jobInfo.QueueType = Convert.ToByte(entity["QueueType"]);
            jobInfo.GroupId = (long)entity["GroupId"];
            jobInfo.Status = (JobStatus)Convert.ToByte(entity["Status"]);
            jobInfo.Definition = (string)entity["Definition"];
            jobInfo.Result = (string)entity["Result"];
            jobInfo.Data = (long?)entity["Data"];
            jobInfo.CancelRequested = (bool)entity["CancelRequested"];
            jobInfo.Version = (long)entity["Version"];
            jobInfo.Priority = (long)entity["Priority"];
            jobInfo.StartDate = ((DateTimeOffset?)entity["StartDate"])?.DateTime;
            jobInfo.CreateDate = ((DateTimeOffset)entity["CreateDate"]).DateTime;
            jobInfo.EndDate = ((DateTimeOffset?)entity["EndDate"])?.DateTime;
            jobInfo.HeartbeatDateTime = ((DateTimeOffset)entity["HeartbeatDateTime"]).DateTime;

            return jobInfo;
        }

        public static DateTime SetKind(this DateTime dateTime, DateTimeKind dateTimeKind)
        {
            return DateTime.SpecifyKind(dateTime, dateTimeKind);
        }
    }
}
