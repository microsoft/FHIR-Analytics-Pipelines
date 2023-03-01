// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public static class FhirToDatalakeJobVersionManager
    {
        public const JobVersion CurrentJobVersion = JobVersion.V4;

        public const JobVersion DefaultJobVersion = JobVersion.V1;

        public static readonly HashSet<JobVersion> SupportedJobVersion = new ()
        {
            JobVersion.V1,
            JobVersion.V2,
            JobVersion.V3,
            JobVersion.V4,
        };

        // The job version is added in input data to handle possible version compatibility issues when the version is updated. It is not related to the job definition, so we need to remove the job version property when calculate job identifier.
        // job version V1
        public static readonly List<string> FhirToDataLakeOrchestratorJobIdentifierPropertiesV1 = new ()
        {
            nameof(FhirToDataLakeOrchestratorJobInputData.JobType),
            nameof(FhirToDataLakeOrchestratorJobInputData.TriggerSequenceId),
            nameof(FhirToDataLakeOrchestratorJobInputData.Since),
            nameof(FhirToDataLakeOrchestratorJobInputData.DataStartTime),
            nameof(FhirToDataLakeOrchestratorJobInputData.DataEndTime),
        };

        public static readonly List<string> FhirToDataLakeProcessingJobIdentifierPropertiesV1 = new ()
        {
            nameof(FhirToDataLakeProcessingJobInputData.JobType),
            nameof(FhirToDataLakeProcessingJobInputData.TriggerSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.ProcessingJobSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.Since),
            nameof(FhirToDataLakeProcessingJobInputData.DataStartTime),
            nameof(FhirToDataLakeProcessingJobInputData.DataEndTime),
            nameof(FhirToDataLakeProcessingJobInputData.ToBeProcessedPatients),
        };

        // job version V2
        // Remove data end time field in Definition to generate job identifier,
        // as the data end time is related to the trigger created time, and may be different if there are two instances try to create a new trigger simultaneously
        public static readonly List<string> FhirToDataLakeOrchestratorJobIdentifierPropertiesV2 = new ()
        {
            nameof(FhirToDataLakeOrchestratorJobInputData.JobType),
            nameof(FhirToDataLakeOrchestratorJobInputData.TriggerSequenceId),
            nameof(FhirToDataLakeOrchestratorJobInputData.Since),
            nameof(FhirToDataLakeOrchestratorJobInputData.DataStartTime),
        };

        public static readonly List<string> FhirToDataLakeProcessingJobIdentifierPropertiesV2 = new ()
        {
            nameof(FhirToDataLakeProcessingJobInputData.JobType),
            nameof(FhirToDataLakeProcessingJobInputData.TriggerSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.ProcessingJobSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.Since),
            nameof(FhirToDataLakeProcessingJobInputData.DataStartTime),
            nameof(FhirToDataLakeProcessingJobInputData.ToBeProcessedPatients),
        };

        // job version V3
        // the same as job version v2
        public static readonly List<string> FhirToDataLakeOrchestratorJobIdentifierPropertiesV3 = new ()
        {
            nameof(FhirToDataLakeProcessingJobInputData.JobType),
            nameof(FhirToDataLakeProcessingJobInputData.TriggerSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.Since),
            nameof(FhirToDataLakeProcessingJobInputData.DataStartTime),
        };

        public static readonly List<string> FhirToDataLakeProcessingJobIdentifierPropertiesV3 = new ()
        {
            nameof(FhirToDataLakeProcessingJobInputData.JobType),
            nameof(FhirToDataLakeProcessingJobInputData.TriggerSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.ProcessingJobSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.Since),
            nameof(FhirToDataLakeProcessingJobInputData.DataStartTime),
            nameof(FhirToDataLakeProcessingJobInputData.ToBeProcessedPatients),
        };

        // job version V4
        // Add split processing job information.
        public static readonly List<string> FhirToDataLakeOrchestratorJobIdentifierPropertiesV4 = new()
        {
            nameof(FhirToDataLakeProcessingJobInputData.JobType),
            nameof(FhirToDataLakeProcessingJobInputData.TriggerSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.Since),
            nameof(FhirToDataLakeProcessingJobInputData.DataStartTime),
        };

        public static readonly List<string> FhirToDataLakeProcessingJobIdentifierPropertiesV4 = new()
        {
            nameof(FhirToDataLakeProcessingJobInputData.JobType),
            nameof(FhirToDataLakeProcessingJobInputData.TriggerSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.ProcessingJobSequenceId),
            nameof(FhirToDataLakeProcessingJobInputData.SplitProcessingJobInfo),
            nameof(FhirToDataLakeProcessingJobInputData.Since),
            nameof(FhirToDataLakeProcessingJobInputData.DataStartTime),
            nameof(FhirToDataLakeProcessingJobInputData.ToBeProcessedPatients),
        };
    }
}
