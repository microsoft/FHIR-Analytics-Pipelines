// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeJobInputDataProperties
    {
        public const string JobType = "JobType";

        public const string JobVersion = "JobVersion";

        public const string TriggerSequenceId = "TriggerSequenceId";

        public const string Since = "Since";

        public const string DataStartTime = "DataStartTime";

        public const string DataEndTime = "DataEndTime";

        public const string ProcessingJobSequenceId = "ProcessingJobSequenceId";

        public const string ToBeProcessedPatients = "ToBeProcessedPatients";
    }
}