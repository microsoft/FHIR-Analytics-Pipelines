// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class DicomToDataLakeAzureStorageJobInfoTests
    {
        [Fact]
        public void GivenTwoDefinitionsWithDifferentEndOffsetForJobVersionV1_WhenGetJobIdentifier_ThenTheJobIdentifierShouldBeTheSame()
        {
            var orchestratorDefinition1 = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                JobVersion = SupportedJobVersion.V1,
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
                JobVersion = SupportedJobVersion.V1,
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
        public void GivenTwoSameDefinitionsForJobVersionV1AndV2_WhenGetJobIdentifier_ThenTheJobIdentifierShouldBeDifferent()
        {
            var orchestratorDefinition1 = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                JobVersion = SupportedJobVersion.V1,
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
                JobVersion = SupportedJobVersion.V2,
                StartOffset = 0,
                EndOffset = 1,
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
            var orchestratorDefinition = new DicomToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                JobVersion = SupportedJobVersion.V1,
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

            jobject[JobVersionManager.JobVersionKey].Parent.Remove();

            var jobComputeObject = jobject.ToString(Formatting.None);

            Assert.Equal(jobComputeObject.ComputeHash(), jobInfo.JobIdentifier());
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

            jobject[JobVersionManager.JobVersionKey].Parent.Remove();

            string inputDataString = JsonConvert.SerializeObject(jobject);

            var deserializedInputData = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobInputData>(inputDataString);

            Assert.Equal(JobVersionManager.DefaultJobVersion, deserializedInputData.JobVersion);
        }
    }
}
