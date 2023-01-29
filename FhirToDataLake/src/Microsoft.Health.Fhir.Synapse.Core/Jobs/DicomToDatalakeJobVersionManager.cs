// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Core.Jobs.Models;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class DicomToDatalakeJobVersionManager
    {
        public const JobVersion CurrentJobVersion = JobVersion.V1;

        public const JobVersion DefaultJobVersion = JobVersion.V1;

        public static readonly HashSet<JobVersion> SupportedJobVersion = new ()
        {
            JobVersion.V1,
        };

        // The job version is added in input data to handle possible version compatibility issues when the version is updated. It is not related to the job definition, so we need to remove the job version property when calculate job identifier.
        // job version V1
        public static readonly List<string> DicomToDataLakeOrchestratorJobIdentifierPropertiesV1 = new ()
        {
            nameof(DicomToDataLakeOrchestratorJobInputData.JobType),
            nameof(DicomToDataLakeOrchestratorJobInputData.TriggerSequenceId),
            nameof(DicomToDataLakeOrchestratorJobInputData.StartOffset),
        };

        public static readonly List<string> DicomToDataLakeProcessingJobIdentifierPropertiesV1 = new ()
        {
            nameof(DicomToDataLakeProcessingJobInputData.JobType),
            nameof(DicomToDataLakeProcessingJobInputData.TriggerSequenceId),
            nameof(DicomToDataLakeProcessingJobInputData.ProcessingJobSequenceId),
            nameof(DicomToDataLakeProcessingJobInputData.StartOffset),
        };
    }
}
