// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public static class JobVersionManager
    {
        public const SupportedJobVersion CurrentJobVersion = SupportedJobVersion.V2;

        public const SupportedJobVersion DefaultJobVersion = SupportedJobVersion.V1;

        // the job version is added in input data to handle possible version compatibility issues when the version is updated. It is not related to the job definition, so we need to remove the job version property when calculate job identifier.
        // job version V1
        public static readonly List<string> FhirToDataLakeOrchestratorJobIdentifierPropertiesV1 = new ()
        {
            FhirToDataLakeJobInputDataProperties.JobType,
            FhirToDataLakeJobInputDataProperties.TriggerSequenceId,
            FhirToDataLakeJobInputDataProperties.Since,
            FhirToDataLakeJobInputDataProperties.DataStartTime,
            FhirToDataLakeJobInputDataProperties.DataEndTime,
        };

        public static readonly List<string> FhirToDataLakeProcessingJobIdentifierPropertiesV1 = new ()
        {
            FhirToDataLakeJobInputDataProperties.JobType,
            FhirToDataLakeJobInputDataProperties.TriggerSequenceId,
            FhirToDataLakeJobInputDataProperties.ProcessingJobSequenceId,
            FhirToDataLakeJobInputDataProperties.Since,
            FhirToDataLakeJobInputDataProperties.DataStartTime,
            FhirToDataLakeJobInputDataProperties.DataEndTime,
            FhirToDataLakeJobInputDataProperties.ToBeProcessedPatients,
        };

        // job version V2
        // Remove data end time field in Definition to generate job identifier,
        // as the data end time is related to the trigger created time, and may be different if there are two instances try to create a new trigger simultaneously
        public static readonly List<string> FhirToDataLakeOrchestratorJobIdentifierPropertiesV2 = new ()
        {
            FhirToDataLakeJobInputDataProperties.JobType,
            FhirToDataLakeJobInputDataProperties.TriggerSequenceId,
            FhirToDataLakeJobInputDataProperties.Since,
            FhirToDataLakeJobInputDataProperties.DataStartTime,
        };

        public static readonly List<string> FhirToDataLakeProcessingJobIdentifierPropertiesV2 = new ()
        {
            FhirToDataLakeJobInputDataProperties.JobType,
            FhirToDataLakeJobInputDataProperties.TriggerSequenceId,
            FhirToDataLakeJobInputDataProperties.ProcessingJobSequenceId,
            FhirToDataLakeJobInputDataProperties.Since,
            FhirToDataLakeJobInputDataProperties.DataStartTime,
            FhirToDataLakeJobInputDataProperties.ToBeProcessedPatients,
        };
    }
}
