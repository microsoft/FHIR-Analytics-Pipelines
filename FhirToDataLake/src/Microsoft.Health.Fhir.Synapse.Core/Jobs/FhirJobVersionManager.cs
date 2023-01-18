// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public static class FhirJobVersionManager
    {
        public const SupportedJobVersion CurrentJobVersion = SupportedJobVersion.V2;

        public const SupportedJobVersion DefaultJobVersion = SupportedJobVersion.V1;

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
    }
}
