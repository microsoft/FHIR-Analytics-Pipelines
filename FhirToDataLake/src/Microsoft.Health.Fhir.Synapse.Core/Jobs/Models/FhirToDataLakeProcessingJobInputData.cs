// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeProcessingJobInputData
    {
        /// <summary>
        /// Job type
        /// </summary>
        public JobType JobType { get; set; }

        /// <summary>
        /// Job version, used for version update
        /// </summary>
        public SupportedJobVersion JobVersion { get; set; } = JobVersionManager.DefaultJobVersion;

        /// <summary>
        /// Trigger sequence id
        /// </summary>
        public long TriggerSequenceId { get; set; }

        /// <summary>
        /// Processing sequence id
        /// </summary>
        public long ProcessingJobSequenceId { get; set; }

        /// <summary>
        /// The start timestamp specified in job configuration.
        /// </summary>
        public DateTimeOffset? Since { get; set; }

        /// <summary>
        /// Resource type and time range parameters for system scope.
        /// SplitParameters will be null for group scope.
        /// </summary>
        public Dictionary<string, TimeRange> SplitParameters { get; set; }

        /// <summary>
        /// Start time, process all data if not specified. Used for group scope.
        /// </summary>
        public DateTimeOffset? DataStartTime { get; set; }

        /// <summary>
        /// End time. Used for group scope.
        /// </summary>
        public DateTimeOffset DataEndTime { get; set; }

        /// <summary>
        /// The patients to be processed, patient id hash and processed patient version are provided for each patient, used for Group scope
        /// </summary>
        public List<PatientWrapper> ToBeProcessedPatients { get; set; }
    }
}