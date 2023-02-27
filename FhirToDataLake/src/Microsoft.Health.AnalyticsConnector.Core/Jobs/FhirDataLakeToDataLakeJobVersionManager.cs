// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.AnalyticsConnector.Core.Jobs.Models;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public static class FhirDataLakeToDataLakeJobVersionManager
    {
        public const JobVersion CurrentJobVersion = JobVersion.V1;

        public const JobVersion DefaultJobVersion = JobVersion.V1;

        public static readonly HashSet<JobVersion> SupportedJobVersion = new ()
        {
            JobVersion.V1,
        };

        // The job version is added in input data to handle possible version compatibility issues when the version is updated. It is not related to the job definition, so we need to remove the job version property when calculate job identifier.
        // job version V1
        public static readonly List<string> OrchestratorJobIdentifierPropertiesV1 = new ()
        {
            nameof(FhirDataLakeToDataLakeOrchestratorJobInputData.JobType),
            nameof(FhirDataLakeToDataLakeOrchestratorJobInputData.TriggerSequenceId),
            nameof(FhirDataLakeToDataLakeOrchestratorJobInputData.DataStartTime),
        };

        public static readonly List<string> ProcessingJobIdentifierPropertiesV1 = new ()
        {
            nameof(FhirDataLakeToDataLakeProcessingJobInputData.JobType),
            nameof(FhirDataLakeToDataLakeProcessingJobInputData.TriggerSequenceId),
            nameof(FhirDataLakeToDataLakeProcessingJobInputData.BlobName),
            nameof(FhirDataLakeToDataLakeProcessingJobInputData.ETag),
        };
    }
}
