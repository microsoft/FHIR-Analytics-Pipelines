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
    public class FhirToDataLakeAzureStorageJobInfoTests
    {
        [Fact]
        public void GivenTwoDefinitionsWithDifferentEndTimeForJobVersionV2_WhenGetJobIdentifier_ThenTheJobIdentifierShouldBeTheSame()
        {
            var orchestratorDefinition1 = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = SupportedJobVersion.V2,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.MinValue,
                DataStartTime = DateTimeOffset.MinValue,
                DataEndTime = DateTimeOffset.UtcNow,
            };

            var jobInfo1 = new FhirToDataLakeAzureStorageJobInfo()
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

            var orchestratorDefinition2 = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = SupportedJobVersion.V2,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.MinValue,
                DataStartTime = DateTimeOffset.MinValue,
                DataEndTime = DateTimeOffset.UtcNow,
            };

            var jobInfo2 = new FhirToDataLakeAzureStorageJobInfo()
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
        public void GivenTwoDefinitionsWithDifferentEndTimeForJobVersionV1_WhenGetJobIdentifier_ThenTheJobIdentifierShouldBeDifferent()
        {
            var orchestratorDefinition1 = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = SupportedJobVersion.V1,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.MinValue,
                DataStartTime = DateTimeOffset.MinValue,
                DataEndTime = DateTimeOffset.UtcNow,
            };

            var jobInfo1 = new FhirToDataLakeAzureStorageJobInfo()
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

            // the default job version is V1
            var orchestratorDefinition2 = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = SupportedJobVersion.V1,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.MinValue,
                DataStartTime = DateTimeOffset.MinValue,
                DataEndTime = DateTimeOffset.UtcNow,
            };

            var jobInfo2 = new FhirToDataLakeAzureStorageJobInfo()
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
            Assert.NotEqual(jobInfo1.JobIdentifier(), jobInfo2.JobIdentifier());
        }

        [Fact]
        public void GivenTwoSameDefinitionsForJobVersionV1AndV2_WhenGetJobIdentifier_ThenTheJobIdentifierShouldBeDifferent()
        {
            var orchestratorDefinition = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = SupportedJobVersion.V1,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.MinValue,
                DataStartTime = DateTimeOffset.MinValue,
                DataEndTime = DateTimeOffset.UtcNow,
            };

            var jobInfo1 = new FhirToDataLakeAzureStorageJobInfo()
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

            orchestratorDefinition.JobVersion = SupportedJobVersion.V2;

            var jobInfo2 = new FhirToDataLakeAzureStorageJobInfo()
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
            Assert.NotEqual(jobInfo1.JobIdentifier(), jobInfo2.JobIdentifier());
        }

        [Fact]
        public void
            GivenJobVersionV1JobDefinition_WhenGetJobIdentifier_ThenTheJobVersionShouldBeRemoved()
        {
            var orchestratorDefinition = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = SupportedJobVersion.V1,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.MinValue,
                DataStartTime = DateTimeOffset.MinValue,
                DataEndTime = DateTimeOffset.UtcNow,
            };

            var jobInfo = new FhirToDataLakeAzureStorageJobInfo()
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
            jobject[JobVersionManager.JobVersionKey].Parent.Remove();

            Assert.Equal(jobject.ToString().ComputeHash(), jobInfo.JobIdentifier());
        }
    }
}
