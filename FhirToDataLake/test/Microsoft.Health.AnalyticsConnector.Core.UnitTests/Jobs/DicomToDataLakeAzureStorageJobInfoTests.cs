// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.AnalyticsConnector.Core.Jobs;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;
using Microsoft.Health.AnalyticsConnector.JobManagement.Extensions;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.Jobs
{
    public class DicomToDataLakeAzureStorageJobInfoTests
    {
        private const string JobVersionKey = nameof(DicomToDataLakeOrchestratorJobInputData.JobVersion);

        [Fact]
        public void GivenTwoDefinitionsWithDifferentEndOffsetForJobVersionV1_WhenGetJobIdentifier_ThenTheJobIdentifierShouldBeTheSame()
        {
            var orchestratorDefinition1 = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                JobVersion = JobVersion.V1,
                StartOffset = 0,
                EndOffset = 1,
            };

            var jobInfo1 = new DicomToDataLakeAzureStorageJobInfo()
            {
                Id = 1L,
                QueueType = 0,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = JsonConvert.SerializeObject(orchestratorDefinition1),
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };

            var orchestratorDefinition2 = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                JobVersion = JobVersion.V1,
                StartOffset = 0,
                EndOffset = 2,
            };

            var jobInfo2 = new DicomToDataLakeAzureStorageJobInfo()
            {
                Id = 1L,
                QueueType = 0,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = JsonConvert.SerializeObject(orchestratorDefinition2),
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };
            Assert.Equal(jobInfo1.JobIdentifier(), jobInfo2.JobIdentifier());
        }

        [Fact]
        public void
       GivenJobVersionV1JobDefinition_WhenGetJobIdentifier_ThenTheJobVersionShouldBeRemoved()
        {
            // The identifier value of job v1: "{"JobType":0,"TriggerSequenceId":1,"Since":"1970-01-01T00:00:00+00:00","StartOffset":0}"
            // you should NOT modify the expectedJobV1Identifier
            var expectedJobV1Identifier = "fc252cc9c1469e41ebbf4ea5a949b8e0d690cf7949c44101cde90c6fd2915739";

            var orchestratorDefinition = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                JobVersion = JobVersion.V1,
                StartOffset = 0,
            };

            var jobInfo = new DicomToDataLakeAzureStorageJobInfo()
            {
                Id = 1L,
                QueueType = 0,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = JsonConvert.SerializeObject(orchestratorDefinition),
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };

            var jobject = JObject.FromObject(orchestratorDefinition);

            Assert.NotEqual(jobject.ToString(Formatting.None).ComputeHash(), jobInfo.JobIdentifier());
            Assert.Equal(expectedJobV1Identifier, jobInfo.JobIdentifier());
        }

        [Fact]
        public void GivenJobInfoWithoutJobVersion_WhenParseStringToInputData_ThenTheDefaultJobVersionIsAssigned()
        {
            var orchestratorDefinition = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                StartOffset = 0,
            };

            var jobject = JObject.FromObject(orchestratorDefinition);

            jobject[JobVersionKey].Parent.Remove();

            string inputDataString = JsonConvert.SerializeObject(jobject);

            var deserializedInputData = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobInputData>(inputDataString);

            Assert.Equal(DicomToDatalakeJobVersionManager.DefaultJobVersion, deserializedInputData.JobVersion);
        }
    }
}
