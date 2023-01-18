// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;
using Microsoft.Health.JobManagement;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Core.UnitTests.Jobs
{
    public class FhirToDataLakeAzureStorageJobInfoTests
    {
        private readonly string _sinceStr = "1970-01-01T00:00:00+00:00";
        private readonly string _startStr = "2000-01-01T00:00:00+00:00";
        private readonly string _endStr = "2023-01-01T00:00:00+00:00";
        private readonly List<PatientWrapper> _toBeProcessedPatients = new () { new PatientWrapper("patient1Hash", 2), new PatientWrapper("patient2Hash", 1) };
        private readonly string _jobVersionPropertyName = nameof(FhirToDataLakeOrchestratorJobInputData.JobVersion);

        [Fact]
        public void GivenTwoDefinitionsWithDifferentEndTimeForJobVersionV2_WhenGetJobIdentifier_ThenTheJobIdentifierShouldBeTheSame()
        {
            var orchestratorDefinition1 = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = JobVersion.V2,
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
                JobVersion = JobVersion.V2,
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
                JobVersion = JobVersion.V1,
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
                JobVersion = JobVersion.V1,
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
            // job v2 remove data end time field when calculate job identifier, so their identifier is different
            var orchestratorDefinition = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = JobVersion.V1,
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

            orchestratorDefinition.JobVersion = JobVersion.V2;

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
       GivenJobVersionV1OrchestratorJobDefinition_WhenGetJobIdentifierInNewVersionCode_ThenTheJobIdentifierShouldNotBeChanged()
        {
            // The job identifier value of job v1 : "{"JobType":0,"TriggerSequenceId":1,"Since":"1970-01-01T00:00:00+00:00","DataStartTime":"2000-01-01T00:00:00+00:00","DataEndTime":"2023-01-01T00:00:00+00:00"}"
            // you should NOT modify the expectedJobV1Identifier
            var expectedJobV1Identifier = "e45dc8b86cc6efade9886b67e1ff8747c60f59e8980d1c69caa2e4041409b199";

            var orchestratorDefinitionWithV1 = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = JobVersion.V1,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.Parse(_sinceStr),
                DataStartTime = DateTimeOffset.Parse(_startStr),
                DataEndTime = DateTimeOffset.Parse(_endStr),
            };

            var jobInfoWithV1 = new FhirToDataLakeAzureStorageJobInfo()
            {
                Id = 1L,
                QueueType = 0,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = JsonConvert.SerializeObject(orchestratorDefinitionWithV1),
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };

            Assert.Equal(expectedJobV1Identifier, jobInfoWithV1.JobIdentifier());

            var orchestratorDefinitionWhitoutJobVersion = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.Parse(_sinceStr),
                DataStartTime = DateTimeOffset.Parse(_startStr),
                DataEndTime = DateTimeOffset.Parse(_endStr),
            };

            var jobInfoWithoutJobVersion = new FhirToDataLakeAzureStorageJobInfo()
            {
                Id = 1L,
                QueueType = 0,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = JsonConvert.SerializeObject(orchestratorDefinitionWhitoutJobVersion),
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };

            Assert.Equal(expectedJobV1Identifier, jobInfoWithoutJobVersion.JobIdentifier());
        }

        [Fact]
        public void
GivenJobVersionV1ProcessingJobDefinition_WhenGetJobIdentifierInNewVersionCode_ThenTheJobIdentifierShouldNotBeChanged()
        {
            // the json string of job v1 : "{"JobType":1,"TriggerSequenceId":1,"ProcessingJobSequenceId":1,"Since":"1970-01-01T00:00:00+00:00","DataStartTime":"2000-01-01T00:00:00+00:00","DataEndTime":"2023-01-01T00:00:00+00:00","ToBeProcessedPatients":[{"patientHash":"patient1Hash","versionId":2},{"patientHash":"patient2Hash","versionId":1}]}"
            // you should NOT modify the expectedJobV1Identifier
            var expectedJobV1Identifier = "6d1f9945e30fac9d7eb1697e1a334b3f735fcfa43e24cafb3a3fe7dce2a796ff";

            var processingDefinitionWithV1 = new FhirToDataLakeProcessingJobInputData
            {
                JobType = JobType.Processing,
                JobVersion = JobVersion.V1,
                TriggerSequenceId = 1,
                ProcessingJobSequenceId = 1,
                Since = DateTimeOffset.Parse(_sinceStr),
                DataStartTime = DateTimeOffset.Parse(_startStr),
                DataEndTime = DateTimeOffset.Parse(_endStr),
                ToBeProcessedPatients = _toBeProcessedPatients,
            };

            var jobInfoWithV1 = new FhirToDataLakeAzureStorageJobInfo()
            {
                Id = 1L,
                QueueType = 0,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = JsonConvert.SerializeObject(processingDefinitionWithV1),
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };

            Assert.Equal(expectedJobV1Identifier, jobInfoWithV1.JobIdentifier());

            var processingDefinitionWhitoutJobVersion = new FhirToDataLakeProcessingJobInputData
            {
                JobType = JobType.Processing,
                TriggerSequenceId = 1,
                ProcessingJobSequenceId = 1,
                Since = DateTimeOffset.Parse(_sinceStr),
                DataStartTime = DateTimeOffset.Parse(_startStr),
                DataEndTime = DateTimeOffset.Parse(_endStr),
                ToBeProcessedPatients = _toBeProcessedPatients,
            };

            var jobInfoWithoutJobVersion = new FhirToDataLakeAzureStorageJobInfo()
            {
                Id = 1L,
                QueueType = 0,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = JsonConvert.SerializeObject(processingDefinitionWhitoutJobVersion),
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };

            Assert.Equal(expectedJobV1Identifier, jobInfoWithoutJobVersion.JobIdentifier());
        }

        [Fact]
        public void
GivenJobVersionV2OrchestratorJobDefinition_WhenGetJobIdentifierInNewVersionCode_ThenTheJobIdentifierShouldNotBeChanged()
        {
            // the json string of job v1 : "{"JobType":0,"TriggerSequenceId":1,"Since":"1970-01-01T00:00:00+00:00","DataStartTime":"2000-01-01T00:00:00+00:00"}"
            // you should NOT modify the expectedJobV1Identifier
            var expectedJobV2Identifier = "bfffa195641029bad6e569cdaf52c75e4db659d03d515952800dc23267513f24";

            var orchestratorDefinition = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                JobVersion = JobVersion.V2,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.Parse(_sinceStr),
                DataStartTime = DateTimeOffset.Parse(_startStr),
                DataEndTime = DateTimeOffset.Parse(_endStr),
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

            Assert.Equal(expectedJobV2Identifier, jobInfo.JobIdentifier());
        }

        [Fact]
        public void
GivenJobVersionV2ProcessingJobDefinition_WhenGetJobIdentifierInNewVersionCode_ThenTheJobIdentifierShouldNotBeChanged()
        {
            // the json string of job v1 : "{"JobType":1,"TriggerSequenceId":1,"ProcessingJobSequenceId":1,"Since":"1970-01-01T00:00:00+00:00","DataStartTime":"2000-01-01T00:00:00+00:00","ToBeProcessedPatients":[{"patientHash":"patient1Hash","versionId":2},{"patientHash":"patient2Hash","versionId":1}]}"
            // you should NOT modify the expectedJobV1Identifier
            var expectedJobV1Identifier = "9c3ec95655e13aed0369ebf9f5a61b3f8631f50ab211b7dca1ae9fca3d44d4e4";

            var processingDefinition = new FhirToDataLakeProcessingJobInputData
            {
                JobType = JobType.Processing,
                JobVersion = JobVersion.V2,
                TriggerSequenceId = 1,
                ProcessingJobSequenceId = 1,
                Since = DateTimeOffset.Parse(_sinceStr),
                DataStartTime = DateTimeOffset.Parse(_startStr),
                DataEndTime = DateTimeOffset.Parse(_endStr),
                ToBeProcessedPatients = _toBeProcessedPatients,
            };

            var jobInfo = new FhirToDataLakeAzureStorageJobInfo()
            {
                Id = 1L,
                QueueType = 0,
                Status = JobStatus.Created,
                GroupId = 0,
                Definition = JsonConvert.SerializeObject(processingDefinition),
                Result = string.Empty,
                CancelRequested = false,
                CreateDate = DateTime.UtcNow,
                HeartbeatDateTime = DateTime.UtcNow,
            };

            var a = jobInfo.JobIdentifier();
            Assert.Equal(expectedJobV1Identifier, jobInfo.JobIdentifier());
        }

        [Fact]
        public void GivenJobInfoWithoutJobVersion_WhenParseStringToInputData_ThenTheDefaultJobVersionIsAssigned()
        {
            var inputData = new FhirToDataLakeOrchestratorJobInputData
            {
                JobType = JobType.Orchestrator,
                TriggerSequenceId = 1,
                Since = DateTimeOffset.MinValue,
                DataStartTime = DateTimeOffset.MinValue,
                DataEndTime = DateTimeOffset.UtcNow,
            };

            var jobject = JObject.FromObject(inputData);

            jobject[_jobVersionPropertyName].Parent.Remove();

            string inputDataString = JsonConvert.SerializeObject(jobject);

            var deserializedInputData = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobInputData>(inputDataString);

            Assert.Equal(FhirJobVersionManager.DefaultJobVersion, deserializedInputData.JobVersion);
        }
    }
}
